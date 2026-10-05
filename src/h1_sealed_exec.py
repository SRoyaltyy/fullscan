"""Webull paper orders from the sealed h1 forward book.

The source of truth is ``research/hot_n4_clean_v4/forward_h1/h1_log.jsonl``.
This module only reads that log. It does not import Factor Mine, does
not call ``pick_day``, and does not write the log or the ledger.

The plan card lists every pick, including names the book already holds.
The open fill does not buy those. It sells ``planned_sells`` first, then
buys only picks that are still not held — the same rule as
``forward.openfill.open_legs`` / ``planfill.fill_book``. That set is the
new-buy set. A carry name is not sized.

Sell share counts are the plan's ``planned_sells`` (and must match the
sealed open fill's sells when that fill exists).

Buy share counts:

* When the session already has an effective open fill (a later
  ``open_fill_correction`` replaces the sealed line in memory only),
  the shares are that fill's shares. The fill's buy names must equal
  the new-buy set, or this refuses.
* When the fill is not sealed yet (the pre-open send), shares are the
  book's sizing on the new-buy names only: ``cash_before`` plus
  estimated sell proceeds at the prior lot's ``last_px``, equal-split
  leftover, price = ``fv_price``, 0.5% slip, 1% ADV cap, Futubull fees.
  Session D's open is not read. Carry names are left out of the split.

A buy list that includes a carry name, or that does not equal the
new-buy set, fails closed. The caller sends the sealed new-buy shares
when paper cash covers them, and fails closed when it does not. It does
not rebuild HOT4.
"""
from __future__ import annotations

import hashlib
import json
import math
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
H1_LOG = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1" / "h1_log.jsonl"
FEES_PATH = ROOT / "00_grounding" / "futubull_fees.json"

# Locked with research/hot_n4_clean_v4/protocol.py. Inlined so the paper
# send path does not import the research package (CI sparse checkouts).
SLIP_PRIMARY = 0.005
LIQ_CAP_FRAC = 0.01
ADV_SHARE_SCALE = 1000

_STATE_KINDS = frozenset({"session", "fill", "open_fill", "mark"})


class SealedH1Error(ValueError):
    """The sealed log cannot be used. Nothing was written."""


def canonical_bytes(obj: dict) -> bytes:
    raw = json.dumps(
        obj, ensure_ascii=False, separators=(",", ":"),
        sort_keys=True, allow_nan=False,
    )
    return (raw + "\n").encode("utf-8")


def _order_fees(shares: int, price: float, side: str, fees: dict) -> float:
    """Futubull US-stock fee. Same formula as ``paper_trade.order_fees``."""
    if shares <= 0 or price <= 0:
        return 0.0
    amount = shares * price
    comm = min(
        max(fees["commission_per_share"] * shares, fees["commission_min_per_order"]),
        fees["commission_max_pct_of_amount"] * amount,
    )
    plat = min(
        max(fees["platform_per_share"] * shares, fees["platform_min_per_order"]),
        fees["platform_max_pct_of_amount"] * amount,
    )
    settle = fees["settlement_per_share"] * shares
    total = comm + plat + settle
    if side == "sell":
        reg = max(
            fees["regulatory_pct_of_amount_sell_only"] * amount,
            fees["regulatory_min_per_order"],
        )
        taf = min(
            max(fees["taf_per_share_sell_only"] * shares, fees["taf_min_per_order"]),
            fees["taf_max_per_order"],
        )
        total += reg + taf
    return round(total, 4)


def _buy_shares(budget: float, cash: float, px: float,
                fv_price, fv_adv, fees: dict) -> tuple[int, str | None]:
    """Whole shares. Same loop as ``run_study.buy_shares``."""
    if fv_price is None or fv_adv is None or fv_price <= 0 or fv_adv <= 0:
        return 0, "missing Finviz Price or Average Volume"
    if px <= 0 or not math.isfinite(px):
        return 0, "missing plan price"
    cap_dollars = LIQ_CAP_FRAC * float(fv_price) * float(fv_adv) * ADV_SHARE_SCALE
    cap_shares = int(math.floor(cap_dollars / px + 1e-12))
    if cap_shares < 1:
        return 0, "cap below one share"
    room = min(float(budget), float(cash))
    gross = px * (1.0 + SLIP_PRIMARY)
    shares = int(math.floor(room / gross + 1e-12))
    shares = min(shares, cap_shares)
    while shares >= 1:
        fee = _order_fees(shares, px, "buy", fees)
        cost = shares * gross + fee
        if cost <= room + 1e-6:
            return shares, None
        shares -= 1
    return 0, "cannot buy one share"


def _load_fees() -> dict:
    return json.loads(FEES_PATH.read_text(encoding="utf-8"))


def _check_line(raw: bytes, obj: dict) -> None:
    if canonical_bytes(obj) != raw:
        raise SealedH1Error("sealed h1 log line is not canonical; refusing")
    body = {key: value for key, value in obj.items() if key != "sha256"}
    digest = hashlib.sha256(canonical_bytes(body)).hexdigest()
    if digest != obj.get("sha256"):
        raise SealedH1Error("sealed h1 plan hash does not match; refusing")


def read_h1_records(log_path: Path | None = None) -> list[tuple[bytes, dict]]:
    """Read the sealed log. Does not write it."""
    path = Path(log_path) if log_path is not None else H1_LOG
    if not path.is_file():
        raise SealedH1Error(f"sealed h1 log missing at {path}; refusing")
    text = path.read_bytes()
    if b"\r" in text:
        raise SealedH1Error("sealed h1 log has CR; refusing")
    if text and not text.endswith(b"\n"):
        raise SealedH1Error("sealed h1 log has no trailing newline; refusing")
    out: list[tuple[bytes, dict]] = []
    for raw in text.splitlines(keepends=True):
        if raw == b"\n" or not raw.endswith(b"\n"):
            raise SealedH1Error("blank sealed h1 log line; refusing")
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise SealedH1Error("sealed h1 log line is not json; refusing") from exc
        if not isinstance(obj, dict):
            raise SealedH1Error("sealed h1 log line is not an object; refusing")
        out.append((raw, obj))
    return out


def load_sealed_h1_plan(date: str, *, log_path: Path | None = None) -> dict | None:
    """The ``kind=plan`` line for ``date``, or None when that session is unsealed.

    A duplicate plan or a line whose hash does not match fails closed.
    """
    found: list[dict] = []
    for raw, obj in read_h1_records(log_path):
        if obj.get("kind") != "plan" or obj.get("date") != date:
            continue
        _check_line(raw, obj)
        found.append(obj)
    if not found:
        return None
    if len(found) != 1:
        raise SealedH1Error(f"duplicate sealed h1 plan for {date}; refusing")
    plan = found[0]
    if not isinstance(plan.get("picks"), list) or not isinstance(plan.get("planned_sells"), list):
        raise SealedH1Error(f"sealed h1 plan {date} has no pick list; refusing")
    return plan


def _state_before(records: list[tuple[bytes, dict]], date: str) -> dict | None:
    """Last sealed holdings record before session ``date``'s plan. Read-only."""
    last = None
    for _raw, obj in records:
        if obj.get("kind") == "plan" and obj.get("date") == date:
            break
        kind = obj.get("kind")
        if kind in _STATE_KINDS and isinstance(obj.get("holdings"), list):
            last = obj
        elif (
            kind == "open_fill_correction"
            and isinstance(obj.get("corrected"), dict)
            and isinstance(obj["corrected"].get("holdings"), list)
        ):
            last = obj["corrected"]
    return last


def _effective_open_fill(records: list[tuple[bytes, dict]], date: str) -> dict | None:
    """Open fill readers use. A correction replaces the sealed line in memory only.

    The sealed line is not rewritten. Same rule as
    ``forward.ledger.effective_open_fill``.
    """
    sealed = None
    correction = None
    for raw, obj in records:
        if obj.get("date") != date:
            continue
        kind = obj.get("kind")
        if kind == "open_fill":
            _check_line(raw, obj)
            if sealed is not None:
                raise SealedH1Error(
                    f"duplicate sealed h1 open fill for {date}; refusing"
                )
            sealed = obj
        elif kind == "open_fill_correction":
            _check_line(raw, obj)
            correction = obj
    if correction is None:
        return sealed
    corrected = correction.get("corrected")
    if not isinstance(corrected, dict) or not isinstance(corrected.get("buys"), list):
        raise SealedH1Error(
            f"sealed h1 open-fill correction for {date} has no buys; refusing"
        )
    if corrected.get("date") not in (None, date):
        raise SealedH1Error(
            f"sealed h1 open-fill correction for {date} is for another session"
        )
    body = dict(corrected)
    if correction.get("sha256"):
        body["sha256"] = correction["sha256"]
    return body


def assert_buys_match_new_set(buys, *, carry, expected) -> None:
    """Refuse a buy list that is not the sealed new-buy set.

    A ticker still held after the session's planned sells is a carry name.
    Sizing it as a buy fails closed. So does any other disagreement with
    ``expected`` (the picks the open fill would actually buy).
    """
    carry_names = {str(ticker).upper() for ticker in carry}
    got = [str(row["ticker"]).upper() for row in buys]
    bad = [ticker for ticker in got if ticker in carry_names]
    if bad:
        raise SealedH1Error(
            f"sealed h1 would buy carry name(s) {', '.join(bad)}; "
            "refusing; not rebuilding HOT4"
        )
    want = [str(ticker).upper() for ticker in expected]
    if got != want:
        raise SealedH1Error(
            f"sealed h1 buys {got} do not match the new-buy set {want}; "
            "refusing; not rebuilding HOT4"
        )


def _held_lots(prior: dict) -> dict:
    held = {}
    for lot in prior.get("holdings") or []:
        if isinstance(lot, dict) and lot.get("ticker"):
            held[str(lot["ticker"]).upper()] = lot
    return held


def _plan_sells(plan: dict, held: dict, fees: dict, date: str) -> tuple[list[dict], float, set[str]]:
    try:
        cash = float(plan.get("cash_before"))
    except (TypeError, ValueError) as exc:
        raise SealedH1Error(f"sealed h1 plan {date} has no cash_before") from exc
    if not math.isfinite(cash) or cash < 0:
        raise SealedH1Error(f"sealed h1 plan {date} cash_before is unusable")
    sells: list[dict] = []
    seen: set[str] = set()
    for row in plan.get("planned_sells") or []:
        if not isinstance(row, dict):
            raise SealedH1Error(f"sealed h1 plan {date} sell is not an object")
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker or ticker in seen:
            raise SealedH1Error(f"sealed h1 plan {date} sell ticker is unusable")
        seen.add(ticker)
        try:
            shares = int(row.get("shares"))
        except (TypeError, ValueError) as exc:
            raise SealedH1Error(
                f"sealed h1 plan {date} sell {ticker} has no shares"
            ) from exc
        if shares < 1:
            raise SealedH1Error(f"sealed h1 plan {date} sell {ticker} shares {shares}")
        lot = held.get(ticker) or {}
        try:
            last_px = float(lot.get("last_px"))
        except (TypeError, ValueError):
            last_px = 0.0
        if last_px > 0 and math.isfinite(last_px):
            fee = _order_fees(shares, last_px, "sell", fees)
            cash += shares * last_px * (1.0 - SLIP_PRIMARY) - fee
        sells.append({
            "ticker": ticker,
            "shares": shares,
            "reason": row.get("reason") or "",
            "px": round(last_px, 4) if last_px > 0 else None,
        })
    return sells, cash, seen


def _split_picks(plan: dict, held_after: set[str], date: str) -> tuple[list[dict], list[str]]:
    """Picks the open fill would buy, and plan picks already held."""
    picks = [row for row in (plan.get("picks") or []) if isinstance(row, dict)]
    picks.sort(key=lambda row: (int(row.get("rank") or 0), str(row.get("ticker") or "")))
    new: list[dict] = []
    carry: list[str] = []
    seen: set[str] = set()
    for pick in picks:
        ticker = str(pick.get("ticker") or "").upper().strip()
        if not ticker or ticker in seen:
            raise SealedH1Error(f"sealed h1 plan {date} pick ticker is unusable")
        seen.add(ticker)
        if ticker in held_after:
            carry.append(ticker)
            continue
        new.append(pick)
    return new, carry


def _size_new_buys(picks: list[dict], cash: float, fees: dict, date: str) -> list[dict]:
    """Equal-split leftover cash across new buys only. Carry names are absent."""
    sized: list[dict] = []
    n = len(picks)
    budgets = [cash / n] * n if n and cash > 0 else [0.0] * n
    running = cash
    for pick, budget in zip(picks, budgets):
        ticker = str(pick.get("ticker") or "").upper().strip()
        try:
            px = float(pick.get("fv_price"))
        except (TypeError, ValueError):
            px = 0.0
        shares, why = _buy_shares(
            budget, running, px, pick.get("fv_price"), pick.get("fv_avg_volume"), fees,
        )
        if why or shares < 1:
            raise SealedH1Error(
                f"sealed h1 pick {ticker} sized to {shares} shares"
                f" ({why or 'zero'}); refusing; not rebuilding HOT4"
            )
        gross = px * (1.0 + SLIP_PRIMARY)
        fee = _order_fees(shares, px, "buy", fees)
        running -= shares * gross + fee
        sized.append({
            "ticker": ticker,
            "shares": int(shares),
            "px": round(px, 4),
            "rank": int(pick.get("rank") or 0),
            "sources": list(pick.get("sources") or []),
        })
    return sized


def _buys_from_open_fill(fill: dict, new_picks: list[dict], sells: list[dict], date: str) -> list[dict]:
    """Share counts the sealed open fill actually bought. Names must match."""
    fill_buys = []
    seen: set[str] = set()
    for row in fill.get("buys") or []:
        if not isinstance(row, dict):
            raise SealedH1Error(f"sealed h1 open fill {date} buy is not an object")
        ticker = str(row.get("ticker") or "").upper().strip()
        if not ticker or ticker in seen:
            raise SealedH1Error(f"sealed h1 open fill {date} buy ticker is unusable")
        seen.add(ticker)
        try:
            shares = int(row.get("shares"))
            px = float(row.get("fill"))
        except (TypeError, ValueError) as exc:
            raise SealedH1Error(
                f"sealed h1 open fill {date} buy {ticker} is unusable"
            ) from exc
        if shares < 1 or not math.isfinite(px) or px <= 0:
            raise SealedH1Error(
                f"sealed h1 open fill {date} buy {ticker} shares {shares}"
            )
        fill_buys.append((ticker, shares, px, row))
    fill_buys.sort(key=lambda item: (
        int((item[3].get("rank") or 0)), item[0],
    ))
    expected = [str(pick.get("ticker") or "").upper() for pick in new_picks]
    got = [ticker for ticker, _shares, _px, _row in fill_buys]
    if got != expected:
        raise SealedH1Error(
            f"sealed h1 open fill buys {got} do not match the new-buy set "
            f"{expected}; refusing; not rebuilding HOT4"
        )
    fill_sells = []
    for row in fill.get("sells") or []:
        if not isinstance(row, dict):
            raise SealedH1Error(f"sealed h1 open fill {date} sell is not an object")
        ticker = str(row.get("ticker") or "").upper().strip()
        try:
            shares = int(row.get("shares"))
        except (TypeError, ValueError) as exc:
            raise SealedH1Error(
                f"sealed h1 open fill {date} sell {ticker} is unusable"
            ) from exc
        fill_sells.append((ticker, shares))
    plan_sells = sorted((row["ticker"], row["shares"]) for row in sells)
    if sorted(fill_sells) != plan_sells:
        raise SealedH1Error(
            f"sealed h1 open fill sells do not match the plan for {date}; "
            "refusing; not rebuilding HOT4"
        )
    by_pick = {str(pick.get("ticker") or "").upper(): pick for pick in new_picks}
    sized = []
    for ticker, shares, px, row in fill_buys:
        pick = by_pick[ticker]
        sized.append({
            "ticker": ticker,
            "shares": shares,
            "px": round(px, 4),
            "rank": int(pick.get("rank") or row.get("rank") or 0),
            "sources": list(pick.get("sources") or row.get("sources") or []),
        })
    return sized


def sealed_h1_orders(date: str, *, log_path: Path | None = None) -> dict:
    """New-buy and sell legs for session ``date``.

    Buys are the sealed new-buy set, in rank order: plan picks that are
    not still held after ``planned_sells``. Carry names are listed on
    ``carry`` and are not sized. Sells are every planned sell.

    When an effective open fill exists, buy shares are that fill's shares.
    Otherwise they are the pre-open sizing of the new-buy names only.
    ``notional`` is the sum of buy shares × the price on each leg (what
    paper cash must cover). This does not resize to a paper account.
    """
    records = read_h1_records(log_path)
    plan = None
    for raw, obj in records:
        if obj.get("kind") == "plan" and obj.get("date") == date:
            _check_line(raw, obj)
            if plan is not None:
                raise SealedH1Error(f"duplicate sealed h1 plan for {date}; refusing")
            plan = obj
    if plan is None:
        raise SealedH1Error(
            f"no sealed h1 plan for {date}; refusing; not rebuilding HOT4"
        )
    fees = _load_fees()
    held = _held_lots(_state_before(records, date) or {})
    sells, cash, sold = _plan_sells(plan, held, fees, date)
    held_after = set(held) - sold
    new_picks, carry = _split_picks(plan, held_after, date)
    fill = _effective_open_fill(records, date)
    if fill is not None:
        sized = _buys_from_open_fill(fill, new_picks, sells, date)
        source = "open_fill"
        fill_sha = fill.get("sha256") or ""
    else:
        sized = _size_new_buys(new_picks, cash, fees, date)
        source = "preopen"
        fill_sha = ""
    assert_buys_match_new_set(
        sized,
        carry=held_after,
        expected=[str(pick.get("ticker") or "").upper() for pick in new_picks],
    )
    notional = round(sum(row["shares"] * row["px"] for row in sized), 2)
    return {
        "date": date,
        "buys": sized,
        "sells": sells,
        "carry": carry,
        "new_buy_source": source,
        "notional": notional,
        "plan_sha256": plan.get("sha256") or "",
        "open_fill_sha256": fill_sha,
        "recipe": plan.get("recipe") or "",
        "morning_s": plan.get("morning_s"),
        "cash_before": plan.get("cash_before"),
    }
