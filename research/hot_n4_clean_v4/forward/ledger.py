"""Append-only hash ledger for the holdup session log.

A record is written once. The sha256 on the record is the hash of the
canonical body with that field removed. The ledger line stores the same
hash and the byte length of the log line. Verification refuses a run when
any line disagrees with the ledger. Nothing here rewrites an earlier line.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from research.hot_n4_clean_v4.forward.book import current_book

HERE = Path(__file__).resolve().parent
LOG_NAME = "holdup_log.jsonl"
LEDGER_NAME = "LEDGER.jsonl"
RECIPE = "union_hot_n4_holdup__w0"


def canonical_bytes(obj: dict) -> bytes:
    raw = json.dumps(obj, ensure_ascii=False, separators=(",", ":"), sort_keys=True, allow_nan=False)
    return (raw + "\n").encode("utf-8")


def sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def log_path(folder: Path | None = None) -> Path:
    book = current_book()
    return (folder or book.folder) / book.log_name


def ledger_path(folder: Path | None = None) -> Path:
    return (folder or current_book().folder) / LEDGER_NAME


def _split(text: bytes, what: str) -> list[bytes]:
    if b"\r" in text:
        raise RuntimeError(f"{what} cr")
    if text and not text.endswith(b"\n"):
        raise RuntimeError(f"{what} newline")
    if not text:
        return []
    lines = text.splitlines(keepends=True)
    for line in lines:
        if line == b"\n" or not line.endswith(b"\n"):
            raise RuntimeError(f"blank {what} line")
    return lines


def seal(body: dict) -> tuple[dict, bytes]:
    """Return the record and the exact log line. ``body`` must not carry sha256."""
    if "sha256" in body:
        raise RuntimeError("sha256 is sealed, not supplied")
    digest = sha256_bytes(canonical_bytes(body))
    record = dict(body)
    record["sha256"] = digest
    line = canonical_bytes(record)
    return record, line


def load(folder: Path | None = None) -> list[dict]:
    """Parse and check every line. Raise if the log and the ledger disagree."""
    folder = current_book().folder if folder is None else folder
    log_lines = _split(log_path(folder).read_bytes() if log_path(folder).is_file() else b"", "log")
    led_lines = _split(ledger_path(folder).read_bytes() if ledger_path(folder).is_file() else b"", "ledger")
    if len(log_lines) != len(led_lines):
        raise RuntimeError("ledger length")
    records = []
    state = _new_state()
    recipe = current_book().recipe
    for seq, (raw, led_raw) in enumerate(zip(log_lines, led_lines), start=1):
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"log line is not json seq {seq}") from exc
        if canonical_bytes(obj) != raw:
            raise RuntimeError(f"log line is not canonical seq {seq}")
        digest = obj.get("sha256")
        body = {key: value for key, value in obj.items() if key != "sha256"}
        if sha256_bytes(canonical_bytes(body)) != digest:
            raise RuntimeError(f"record hash seq {seq}")
        try:
            led = json.loads(led_raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"ledger line is not json seq {seq}") from exc
        if canonical_bytes(led) != led_raw:
            raise RuntimeError(f"ledger line is not canonical seq {seq}")
        if led.get("sha256") != digest or int(led.get("bytes")) != len(raw):
            raise RuntimeError(f"ledger mismatch seq {seq}")
        if int(led.get("seq")) != seq or led.get("kind") != obj.get("kind"):
            raise RuntimeError(f"ledger seq {seq}")
        if led.get("date") != obj.get("date") or led.get("recipe") != recipe:
            raise RuntimeError(f"ledger identity seq {seq}")
        if obj.get("recipe") != recipe:
            raise RuntimeError(f"recipe seq {seq}")
        _note(state, obj)
        records.append(obj)
    _finish(state)
    return records


_PLAN_FORBIDDEN = frozenset({
    "buys", "sells", "unfilled", "holdings", "equity_primary", "cash_primary",
    "fill", "open", "close", "pnl", "pnl_primary",
})
_PICK_FORBIDDEN = frozenset({"fill", "open", "close", "shares", "pnl", "pnl_primary"})
_PLANNED_SELL_FORBIDDEN = frozenset({"fill", "open", "close", "pnl", "pnl_primary"})


def _new_state() -> dict:
    return {
        "close_keys": [],
        "closes": {},
        "fill_dates": [],
        "mark_dates": [],
        "effective_open": {},
        "open_fill_dates": [],
        "open_fills": {},
        "parents": {},
        "missing_dates": [],
        "plan_dates": [],
        "plan_sha": {},
        "plans": {},
        "session_dates": [],
    }


def _state_from(records: list[dict]) -> dict:
    state = _new_state()
    for record in records:
        _note(state, record)
    _finish(state)
    return state


def _reject_keys(row: dict, banned: frozenset, label: str) -> None:
    found = banned.intersection(row)
    if found:
        raise RuntimeError(f"{label} has {sorted(found)[0]}")


def _unique_symbols(buys: list, sells: list) -> None:
    buy_names = [row["ticker"] for row in buys]
    sell_names = [row["ticker"] for row in sells]
    if len(buy_names) != len(set(buy_names)) or len(sell_names) != len(set(sell_names)):
        raise RuntimeError("duplicate fill")


def _note(state: dict, obj: dict) -> None:
    """Accept one sealed record. Raises when the append would break the book."""
    kind = obj.get("kind")
    if kind == "session":
        if state["plan_dates"]:
            raise RuntimeError("session after plan")
        if obj["date"] in state["session_dates"]:
            raise RuntimeError("duplicate session")
        if state["session_dates"] and obj["date"] <= state["session_dates"][-1]:
            raise RuntimeError("session order")
        state["session_dates"].append(obj["date"])
        for buy in obj.get("buys") or []:
            if "pnl" in buy or "pnl_primary" in buy:
                raise RuntimeError("pnl on a buy")
        for sell in obj.get("sells") or []:
            if "pnl" in sell or "pnl_primary" in sell:
                raise RuntimeError("pnl on a sell")
        state["parents"][obj["date"]] = obj
    elif kind == "plan":
        _reject_keys(obj, _PLAN_FORBIDDEN, "plan")
        for pick in obj.get("picks") or []:
            _reject_keys(pick, _PICK_FORBIDDEN, "pick")
        for sell in obj.get("planned_sells") or []:
            _reject_keys(sell, _PLANNED_SELL_FORBIDDEN, "planned sell")
        if not isinstance(obj.get("picks"), list) or not isinstance(obj.get("planned_sells"), list):
            raise RuntimeError("plan shape")
        if not isinstance(obj.get("excluded_unexplained_legs"), list):
            raise RuntimeError("plan exclusions")
        if state["session_dates"] and obj["date"] <= state["session_dates"][-1]:
            raise RuntimeError("plan date")
        if state["plan_dates"] and obj["date"] <= state["plan_dates"][-1]:
            raise RuntimeError("plan order")
        resolved = set(state["fill_dates"]) | set(state["mark_dates"])
        if state["plan_dates"] and state["plan_dates"][-1] not in resolved:
            raise RuntimeError("previous plan is not filled")
        if obj["date"] in state["missing_dates"]:
            raise RuntimeError("plan on a missing session")
        if obj["date"] in state["plan_dates"]:
            raise RuntimeError("duplicate plan")
        state["plan_dates"].append(obj["date"])
        state["plan_sha"][obj["date"]] = obj["sha256"]
        state["plans"][obj["date"]] = obj
    elif kind == "open_fill":
        if "equity_primary" in obj:
            raise RuntimeError("open fill has equity")
        if obj.get("pnl_status") != "pending":
            raise RuntimeError("open fill pnl")
        if obj["date"] not in state["plan_sha"]:
            raise RuntimeError("open fill without plan")
        if obj["date"] in state["open_fill_dates"] or obj["date"] in state["fill_dates"] or obj["date"] in state["mark_dates"]:
            raise RuntimeError("duplicate fill")
        if state["open_fill_dates"] and obj["date"] <= state["open_fill_dates"][-1]:
            raise RuntimeError("open fill order")
        _match_fill(state["plans"][obj["date"]], obj)
        _unique_symbols(obj.get("buys") or [], obj.get("sells") or [])
        state["open_fill_dates"].append(obj["date"])
        state["open_fills"][obj["date"]] = obj
        state["effective_open"][obj["date"]] = obj
    elif kind == "open_fill_correction":
        _note_correction(state, obj)
    elif kind == "fill":
        if obj["date"] not in state["plan_sha"]:
            raise RuntimeError("fill without plan")
        if obj["date"] in state["open_fill_dates"] or obj["date"] in state["mark_dates"]:
            raise RuntimeError("duplicate fill")
        if state["fill_dates"] and obj["date"] <= state["fill_dates"][-1]:
            raise RuntimeError("fill order")
        if obj["date"] in state["fill_dates"]:
            raise RuntimeError("duplicate fill")
        _match_fill(state["plans"][obj["date"]], obj)
        _unique_symbols(obj.get("buys") or [], obj.get("sells") or [])
        state["fill_dates"].append(obj["date"])
        state["parents"][obj["date"]] = obj
    elif kind == "mark":
        if obj["date"] not in state["open_fills"]:
            raise RuntimeError("mark without open fill")
        if obj["date"] in state["mark_dates"] or obj["date"] in state["fill_dates"]:
            raise RuntimeError("duplicate mark")
        if state["mark_dates"] and obj["date"] <= state["mark_dates"][-1]:
            raise RuntimeError("mark order")
        opened = state["effective_open"][obj["date"]]
        if obj.get("open_fill_sha256") != opened.get("sha256"):
            raise RuntimeError("mark does not match open fill")
        if obj.get("plan_sha256") != state["plan_sha"].get(obj["date"]):
            raise RuntimeError("mark does not match plan")
        if obj.get("pnl_status") != "marked" or "equity_primary" not in obj:
            raise RuntimeError("mark pnl")
        buy_names = {row["ticker"] for row in opened.get("buys") or []}
        sell_names = {row["ticker"] for row in opened.get("sells") or []}
        planned = {row["ticker"]: row for row in state["plans"][obj["date"]].get("planned_sells") or []}
        for row in obj.get("added_buys") or []:
            if row["ticker"] in buy_names:
                raise RuntimeError("duplicate fill")
            buy_names.add(row["ticker"])
        for row in obj.get("added_sells") or []:
            if row["ticker"] in sell_names:
                raise RuntimeError("duplicate fill")
            if row["ticker"] not in planned:
                raise RuntimeError("fill sell is not planned")
            sell_names.add(row["ticker"])
        _unique_symbols(obj.get("added_buys") or [], obj.get("added_sells") or [])
        state["mark_dates"].append(obj["date"])
        parent = dict(obj)
        parent["sells"] = list(opened.get("sells") or []) + list(obj.get("added_sells") or [])
        state["parents"][obj["date"]] = parent
    elif kind == "close":
        key = (obj["date"], obj["ticker"], obj["entry_date"])
        if key in state["close_keys"]:
            raise RuntimeError("duplicate close")
        state["close_keys"].append(key)
        if "pnl_primary" not in obj:
            raise RuntimeError("close without pnl")
        parent = state["parents"].get(obj["date"])
        if parent is None:
            raise RuntimeError("close without session or fill")
        sell_names = {row["ticker"] for row in parent.get("sells") or []}
        if obj["ticker"] not in sell_names:
            raise RuntimeError("close is not a sell")
        state["closes"].setdefault(obj["date"], []).append(obj)
    elif kind == "missing":
        if obj.get("reason") != "missing: plan not sealed before open":
            raise RuntimeError("missing reason")
        if obj.get("picks") or obj.get("planned_sells"):
            raise RuntimeError("missing has picks")
        if not obj.get("committed_at"):
            raise RuntimeError("missing timestamp")
        resolved = set(state["fill_dates"]) | set(state["mark_dates"])
        if state["plan_dates"] and state["plan_dates"][-1] not in resolved:
            raise RuntimeError("previous plan is not filled")
        if state["session_dates"] and obj["date"] <= state["session_dates"][-1]:
            raise RuntimeError("missing date")
        if obj["date"] in state["plan_dates"] or obj["date"] in state["missing_dates"]:
            raise RuntimeError("duplicate missing")
        if state["plan_dates"] and obj["date"] <= state["plan_dates"][-1]:
            raise RuntimeError("missing order")
        if state["missing_dates"] and obj["date"] <= state["missing_dates"][-1]:
            raise RuntimeError("missing order")
        state["missing_dates"].append(obj["date"])
    else:
        raise RuntimeError(f"kind {kind}")


def _note_correction(state: dict, obj: dict) -> None:
    """A later line replaces what readers use for that open fill. The sealed line stays."""
    day = obj.get("date")
    if day not in state["open_fills"]:
        raise RuntimeError("correction without open fill")
    if day in state["fill_dates"] or day in state["mark_dates"]:
        raise RuntimeError("correction after close")
    if any(plan_day > day for plan_day in state["plan_dates"]):
        raise RuntimeError("correction after a later plan")
    if any(session_day > day for session_day in state["session_dates"]):
        raise RuntimeError("correction after a later session")
    sealed = state["open_fills"][day]
    if obj.get("open_fill_sha256") != sealed.get("sha256"):
        raise RuntimeError("correction does not match open fill")
    if obj.get("plan_sha256") != state["plan_sha"].get(day):
        raise RuntimeError("correction does not match plan")
    reason = obj.get("reason")
    approved = obj.get("approved_by")
    if not isinstance(reason, str) or not reason.strip():
        raise RuntimeError("correction reason")
    if not isinstance(approved, str) or not approved.strip():
        raise RuntimeError("correction approved_by")
    corrected = obj.get("corrected")
    if not isinstance(corrected, dict):
        raise RuntimeError("correction shape")
    if corrected.get("kind") != "open_fill" or corrected.get("date") != day:
        raise RuntimeError("correction shape")
    if "equity_primary" in corrected:
        raise RuntimeError("open fill has equity")
    if corrected.get("pnl_status") != "pending":
        raise RuntimeError("open fill pnl")
    if corrected.get("plan_sha256") != obj.get("plan_sha256"):
        raise RuntimeError("correction does not match plan")
    try:
        cash = float(corrected["cash_primary"])
    except (KeyError, TypeError, ValueError) as exc:
        raise RuntimeError("correction cash") from exc
    if cash < 0:
        raise RuntimeError(f"correction cash_primary {cash} is negative")
    if not isinstance(obj.get("changed_legs"), list):
        raise RuntimeError("correction changed_legs")
    for leg in obj["changed_legs"]:
        if not isinstance(leg, dict) or not leg.get("ticker") or leg.get("side") not in ("buy", "sell"):
            raise RuntimeError("correction changed_legs")
    _match_fill(state["plans"][day], corrected)
    _unique_symbols(corrected.get("buys") or [], corrected.get("sells") or [])
    view = dict(corrected)
    view["sha256"] = obj["sha256"]
    state["effective_open"][day] = view


def _match_fill(plan: dict, fill: dict) -> None:
    if fill.get("plan_sha256") != plan.get("sha256"):
        raise RuntimeError("fill does not match plan")
    picks = {row["ticker"]: row for row in plan.get("picks") or []}
    planned = {row["ticker"]: row for row in plan.get("planned_sells") or []}
    buy_names = set()
    for buy in fill.get("buys") or []:
        row = picks.get(buy["ticker"])
        if row is None:
            raise RuntimeError("fill buy is not a pick")
        if int(buy["rank"]) != int(row["rank"]):
            raise RuntimeError("fill rank")
        buy_names.add(buy["ticker"])
    sell_names = set()
    for sell in fill.get("sells") or []:
        row = planned.get(sell["ticker"])
        if row is None:
            raise RuntimeError("fill sell is not planned")
        if sell.get("reason") != row.get("reason") or int(sell["shares"]) != int(row["shares"]):
            raise RuntimeError("fill sell")
        sell_names.add(sell["ticker"])
    unfilled_buys = set()
    unfilled_sells = set()
    for row in fill.get("unfilled") or []:
        side = row.get("side")
        if side == "buy":
            if row["ticker"] not in picks:
                raise RuntimeError("unfilled buy is not a pick")
            unfilled_buys.add(row["ticker"])
        elif side == "sell":
            if row["ticker"] not in planned:
                raise RuntimeError("unfilled sell is not planned")
            unfilled_sells.add(row["ticker"])
        else:
            raise RuntimeError("unfilled side")
    held_after = {row["ticker"] for row in fill.get("holdings") or []}
    for ticker in picks:
        if ticker in buy_names or ticker in unfilled_buys or ticker in held_after:
            continue
        raise RuntimeError("pick missing from fill")
    for ticker in planned:
        if ticker not in sell_names and ticker not in unfilled_sells:
            raise RuntimeError("planned sell missing from fill")


def _finish(state: dict) -> None:
    if state["session_dates"] != sorted(state["session_dates"]):
        raise RuntimeError("session order")
    if state["plan_dates"] != sorted(state["plan_dates"]):
        raise RuntimeError("plan order")
    if state["fill_dates"] != sorted(state["fill_dates"]):
        raise RuntimeError("fill order")
    for day, parent in state["parents"].items():
        if parent.get("kind") not in ("session", "fill", "mark"):
            continue
        sells = [row["ticker"] for row in parent.get("sells") or []]
        closed = [row["ticker"] for row in state["closes"].get(day, [])]
        if sorted(sells) != sorted(closed):
            raise RuntimeError("closes do not match sells")


def open_plan(records: list[dict]) -> dict | None:
    """A plan with no fill and no close mark. An open-fill alone is still open."""
    resolved = {row["date"] for row in records if row["kind"] in ("fill", "mark")}
    for row in records:
        if row["kind"] == "plan" and row["date"] not in resolved:
            return row
    return None


def plan_on(records: list[dict], day: str) -> dict | None:
    for row in records:
        if row["kind"] == "plan" and row["date"] == day:
            return row
    return None


def kind_on(records: list[dict], day: str, kind: str) -> dict | None:
    for row in records:
        if row["kind"] == kind and row["date"] == day:
            return row
    return None


def latest_open_correction(records: list[dict], day: str) -> dict | None:
    """The last ``open_fill_correction`` for ``day``. The sealed open fill is not this."""
    found = None
    for row in records:
        if row.get("kind") == "open_fill_correction" and row.get("date") == day:
            found = row
    return found


def effective_open_fill(records: list[dict], day: str) -> dict | None:
    """Open fill readers use. A correction replaces the sealed line in memory only.

    The sha256 on the result is the correction's when one exists, so a later
    close mark points at the line whose fills it used. The sealed line is
    left as it was written.
    """
    sealed = kind_on(records, day, "open_fill")
    correction = latest_open_correction(records, day)
    if correction is None:
        return sealed
    corrected = correction.get("corrected")
    if not isinstance(corrected, dict):
        return sealed
    body = dict(corrected)
    if correction.get("sha256"):
        body["sha256"] = correction["sha256"]
    return body


def book_dates(records: list[dict]) -> list[str]:
    return [row["date"] for row in records if row["kind"] in ("session", "fill", "mark")]


def session_dates(records: list[dict]) -> list[str]:
    return [row["date"] for row in records if row["kind"] == "session"]


def has_session(records: list[dict], day: str) -> bool:
    return day in session_dates(records)


def append_records(bodies: list[dict], folder: Path | None = None) -> list[dict]:
    """Append new lines. Refuse if the current file does not match the ledger.

    Returns the sealed records. Does not rewrite a byte already on disk.
    """
    if not bodies:
        raise RuntimeError("empty append")
    folder = current_book().folder if folder is None else folder
    folder.mkdir(parents=True, exist_ok=True)
    existing = load(folder)
    start = len(existing)
    state = _state_from(existing)
    log_file = log_path(folder)
    led_file = ledger_path(folder)
    before_log = log_file.read_bytes() if log_file.is_file() else b""
    before_led = led_file.read_bytes() if led_file.is_file() else b""
    log_out = []
    led_out = []
    sealed = []
    for offset, body in enumerate(bodies):
        if body.get("recipe") != current_book().recipe:
            raise RuntimeError("recipe")
        record, line = seal(body)
        _note(state, record)
        seq = start + offset + 1
        led = {
            "bytes": len(line),
            "date": record["date"],
            "kind": record["kind"],
            "recipe": current_book().recipe,
            "seq": seq,
            "sha256": record["sha256"],
        }
        log_out.append(line)
        led_out.append(canonical_bytes(led))
        sealed.append(record)
    _finish(state)
    with log_file.open("ab") as handle:
        for line in log_out:
            handle.write(line)
    with led_file.open("ab") as handle:
        for line in led_out:
            handle.write(line)
    if not log_file.read_bytes().startswith(before_log):
        raise RuntimeError("log prefix changed")
    if not led_file.read_bytes().startswith(before_led):
        raise RuntimeError("ledger prefix changed")
    load(folder)
    return sealed
