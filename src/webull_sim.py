"""Webull simulated books. One new book per strategy, named ``<strategy>_webull_sim``.

Each book reads that strategy's sealed pre-09:30 ET plan, buys whole shares
at the session open, and lets the stop fill before the target when a bar
touches both. Cash, positions, and fees carry day to day inside one section.

2026-10-06 is the first day that can be fingerprinted. Earlier days are
built once, labelled BUILT AFTER THE FACT, and are not mixed into the
locked result. A strategy with no sealed plan still gets a row that says
why. Nothing here writes an existing live book.

The scheduled run stays shut from 03:00 until 09:40 ET.

From 2026-10-07, an excel book whose prior weekday has no final file
sits that session out. A draft is not a plan. A sit-out day still
exits lots the book already holds, buys nothing, and can be final
after 16:00 ET.

Theme Radar research shorts read that repo's pre-open plan file
(``research/shadow_log/plans/plan_<entry date>.csv``), not the shadow log.
The file's first commit has to be before 09:30 ET. A short sells the open
and covers the close of the last hold day.
"""
from __future__ import annotations

import csv
import html
import io
import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from zoneinfo import ZoneInfo

from . import past_day_lock

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
FEES_PATH = ROOT / "00_grounding" / "webull_fees.json"
BOOKS_PATH = ROOT / "data" / "webull_sim" / "days.jsonl"
# Side notes for sealed rows. Keyed by book and date. Not part of the row.
MARK_NOTES_PATH = ROOT / "data" / "webull_sim" / "mark_notes.jsonl"
INTRADAY_MARK_NOTE = "equity is a 10:56 ET intraday mark, not the close"
CLOSE_EQUITY_KIND = "close_equity"
CLOSE_EQUITY_NOTE = "true 16:00 ET close equity; sealed row unchanged"
MD_PATH = ROOT / "03_scoreboard" / "WEBULL_SIM.md"
HTML_PATH = ROOT / "dashboard" / "webull-sim" / "index.html"
H1_LOG = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1" / "h1_log.jsonl"
TICKET_DIR = ROOT / "data" / "day_board"
EXCEL_DIR = ROOT / "excel_bot" / "daily"
EXCEL_STRATS = ROOT / "excel_bot" / "strategies"
EXCEL_FREEZE = ROOT / "excel_bot" / "freeze_manifest.json"
EXCEL_LOCK_FROM = "2026-10-06"
EXCEL_PRE_LOCK = "sealed by git commit time (pre-lock)"
EXCEL_MANIFEST_SEAL = "sealed by git commit time + excel_bot freeze manifest"
# IRONCLAD 26: this session's open versus the previous close.
JUMP_HI = 3.0
JUMP_LO = 1.0 / 3.0
JUMP_TOL = 0.25
# The day's last sim. 09:45 and 10:15 ET retry. 16:15 ET is the only cron
# at or after the 16:00 close, so that run is final.
MISSING_OPEN_RULE = (
    "The 09:45 and 10:15 ET runs retry a name that still has no usable Yahoo "
    "session open. The 16:15 ET run is the day's final run: it is the only "
    "scheduled run at or after the 16:00 ET close. No row becomes final before "
    "that close. A run during market hours writes a draft: the open fills can "
    "show, and the row stays rewritable until the close run. On the final run "
    "a pick with no print, or an open dropped by the unexplained 3x jump check, "
    "is logged \"no open, not filled\". That slot stays cash and the other fills "
    "lock. A planned exit with no open is not sold; the lot stays held and is "
    "logged the same way. The unfilled names are listed on the row. A row that "
    "is already final is left as written."
)
SHADOW_DIR = ROOT / "research" / "forward_shadow_v1" / "ledger"
PAPER_OPEN = ROOT / "data" / "paper_open"
PRICE_PATH = ROOT / "data" / "prices" / "ohlc.parquet"
ACTIONS_PATH = ROOT / "data" / "prices" / "actions.parquet"

FIRST_LOCKED = "2026-10-06"
WATERMARK = "2026-10-05"
# From this session on, a missing prior-weekday excel final is a sit-out row,
# and a sit-out or no-plan day still exits lots already held.
SIT_OUT_FROM = "2026-10-07"
START_CASH = Decimal("10000")
Q = Decimal("0.000001")
RECORD = "webull_sim"

# Return columns are not a borrow rate. Only an explicit rate field counts.
BORROW_FIELDS = ("borrow_rate", "borrow_bps", "borrow_fee", "borrow_per_share")

NO_MORNING_PLAN = (
    (
        "breadth_rank_v1",
        "no pre-09:30 plan",
        "return files say assumed pre-open, not server-proven, and designed_after",
    ),
    (
        "breadth_rank_v1b",
        "no pre-09:30 plan",
        "return files say assumed pre-open, not server-proven, and designed_after",
    ),
    (
        "breadth_rank_v1c",
        "no pre-09:30 plan",
        "return files say assumed pre-open, not server-proven, and designed_after",
    ),
    (
        "lever_search",
        "no pre-09:30 plan",
        "no sealed pre-09:30 order sheet in this repo",
    ),
    (
        "oos0914",
        "no pre-09:30 plan",
        "the state file is the filled book and is committed after the close",
    ),
)


@dataclass
class Schedule:
    commission: Decimal
    sec_per_dollar: Decimal
    cat_per_share: Decimal
    taf_per_share: Decimal
    source_url: str
    retrieved: str


# A plan that does not set a count or weights uses this slot. h1 sets its own count.
SIZING_RULE = (
    "When a plan does not set its own count or weights, each pick gets a slot of "
    "equity ÷ max(20, that day's pick count). Equity is measured at that day's "
    "09:30 open before buys. Carried lots are marked at that open. A prior row's "
    "stored equity is not reused. All of that day's picks are sized. None is dropped "
    "for ranking, and the result does not depend on an unsealed order. "
    "The plan sets no priority. A slot too small for one whole share logs "
    "'no whole share'. 'No cash' happens only when held lots tie up the cash. "
    "Those picks stay in the plan's listed order and are marked "
    "'plan sets no priority'. A plan that sets its own count or weights "
    "(for example h1) keeps that count or those weights."
)


@dataclass
class Pick:
    ticker: str
    side: str
    stop: Decimal | None = None
    target_pct: Decimal | None = None
    hold_sessions: int | None = None
    shares: int | None = None
    weight: Decimal | None = None
    exit_on: str | None = None


@dataclass
class Lot:
    ticker: str
    side: str
    shares: int
    entry_px: Decimal
    entry_date: str
    stop: Decimal | None = None
    target: Decimal | None = None
    exit_index: int | None = None
    exit_on: str | None = None


@dataclass
class Account:
    cash: Decimal = START_CASH
    lots: list[Lot] = field(default_factory=list)
    fees: Decimal = Decimal("0")


def load_schedule(path: Path | None = None) -> Schedule:
    raw = json.loads((path or FEES_PATH).read_text(encoding="utf-8"))
    return Schedule(
        commission=Decimal(str(raw["commission_per_trade"])),
        sec_per_dollar=Decimal(raw["sec_fee"]["rate_per_dollar_of_sale"]),
        cat_per_share=Decimal(raw["cat_fee"]["rate_per_share"]),
        taf_per_share=Decimal(raw["finra_taf"]["per_share"]),
        source_url=raw["source_url"],
        retrieved=raw["retrieved"],
    )


def q(value: Decimal) -> str:
    return format(value.quantize(Q, rounding=ROUND_HALF_UP), "f")


def fee_for(schedule: Schedule, side: str, shares: int, price: Decimal) -> Decimal:
    """Published Webull US-listed schedule. ``side`` is buy or sell."""
    if shares < 0:
        raise ValueError("shares must be whole and non-negative")
    if schedule.commission != 0:
        raise ValueError("published commission is 0; refusing another commission")
    notional = price * Decimal(shares)
    cat = schedule.cat_per_share * Decimal(shares)
    sell = side == "sell"
    sec = schedule.sec_per_dollar * notional if sell else Decimal("0")
    taf = schedule.taf_per_share * Decimal(shares) if sell else Decimal("0")
    return cat + sec + taf


def whole_shares(budget: Decimal, price: Decimal, schedule: Schedule, side: str) -> int:
    """Largest whole-share size whose cost fits in ``budget``."""
    if budget <= 0 or price <= 0:
        return 0
    if side == "buy":
        unit = price + schedule.cat_per_share + schedule.commission
        if unit <= 0:
            return 0
        return int(budget // unit)
    # Short notional is capped at the cash. Sell fees come out of cash.
    return int(budget // price)


def _bar_px(bar: dict | None, key: str) -> Decimal | None:
    if not bar or bar.get(key) is None:
        return None
    return Decimal(str(bar[key]))


def _book_exit(lot: Lot, price: Decimal, schedule: Schedule, reason: str, date: str) -> dict:
    side = "sell" if lot.side == "long" else "buy"
    fee = fee_for(schedule, side, lot.shares, price)
    return {
        "ticker": lot.ticker,
        "side": side,
        "shares": lot.shares,
        "price": price,
        "fee": fee,
        "reason": reason,
        "date": date,
        "position": lot.side,
    }


def equity_at_open(account: Account, bars: dict[str, dict]) -> Decimal:
    """Equity at the 09:30 open, after open exits and before new buys.

    This is cash plus lots at the open. It does not read a stored equity
    figure, including a sealed intraday mark.
    """
    equity = account.cash
    for lot in account.lots:
        px = _bar_px(bars.get(lot.ticker), "open")
        if px is None:
            px = lot.entry_px
        if lot.side == "long":
            equity += px * Decimal(lot.shares)
        else:
            equity += (lot.entry_px - px) * Decimal(lot.shares)
    return equity


def resolve_sizing(picks: list[Pick], sizing: str) -> str:
    """Explicit shares or weights on the plan outrank the default slot."""
    if any(pick.shares is not None for pick in picks):
        return "own_shares"
    if any(pick.weight is not None for pick in picks):
        return "own_weights"
    if sizing == "own_count":
        return "own_count"
    return "slot"


def sizing_label(mode: str, n: int) -> str:
    if mode == "own_count":
        if not n:
            return "plan sets its own count; each slot is equity / that count"
        return f"plan sets its own count ({n}); each slot is equity / {n}"
    if mode == "own_weights":
        return "plan sets its own weights"
    if mode == "own_shares":
        return "plan sets its own share count"
    shown = n if n else "n"
    return (
        f"each slot is equity / max(20, {shown}) at the 09:30 open before buys; "
        "all picks sized; plan sets no priority"
    )


def slot_budgets(equity: Decimal, picks: list[Pick], mode: str) -> list[Decimal]:
    n = len(picks)
    if n == 0:
        return []
    if mode == "own_weights":
        weights = [pick.weight if pick.weight is not None else Decimal("0") for pick in picks]
        total = sum(weights, Decimal("0"))
        if total <= 0:
            return [equity / Decimal(n)] * n
        return [equity * weight / total for weight in weights]
    if mode == "own_count":
        slot = equity / Decimal(n)
        return [slot] * n
    if mode == "own_shares":
        return [Decimal("0")] * n
    slot = equity / Decimal(max(20, n))
    return [slot] * n


def apply_cash_exit(account: Account, lot: Lot, fill: dict) -> None:
    price = fill["price"]
    fee = fill["fee"]
    account.fees += fee
    if lot.side == "long":
        account.cash += price * Decimal(lot.shares) - fee
    else:
        # Fees were taken at entry. Cover pays the difference and the buy fee.
        account.cash += (lot.entry_px - price) * Decimal(lot.shares) - fee


def apply_session(
    account: Account,
    picks: list[Pick],
    exit_tickers: list[str],
    bars: dict[str, dict],
    schedule: Schedule,
    date: str,
    session_index: int,
    sizing: str = "slot",
) -> list[dict]:
    """One session. Open exits and new buys use the open. Stop wins over target."""
    fills: list[dict] = []
    kept: list[Lot] = []
    exits = {t.upper() for t in exit_tickers}
    for lot in account.lots:
        bar = bars.get(lot.ticker)
        opened = _bar_px(bar, "open")
        if opened is None:
            kept.append(lot)
            continue
        reason = None
        if lot.ticker.upper() in exits:
            reason = "plan sell"
        elif lot.exit_index is not None and session_index >= lot.exit_index:
            reason = "hold"
        elif lot.stop is not None and (
            (lot.side == "long" and opened <= lot.stop)
            or (lot.side == "short" and opened >= lot.stop)
        ):
            reason = "stop"
        if reason:
            fill = _book_exit(lot, opened, schedule, reason, date)
            apply_cash_exit(account, lot, fill)
            fills.append(fill)
        else:
            kept.append(lot)
    account.lots = kept

    # Listed order is the plan's order. It is not a rank. Every pick is sized.
    mode = resolve_sizing(picks, sizing)
    equity = equity_at_open(account, bars)
    budgets = slot_budgets(equity, picks, mode)
    for pick, budget in zip(picks, budgets):
        bar = bars.get(pick.ticker)
        opened = _bar_px(bar, "open")
        entry_side = "buy" if pick.side == "long" else "sell"
        if opened is None:
            fills.append({
                "ticker": pick.ticker, "side": pick.side, "shares": 0,
                "price": None, "fee": Decimal("0"), "reason": "open not observed",
                "date": date,
            })
            continue
        if mode == "own_shares":
            requested = int(pick.shares or 0)
            slot_shares = requested if requested > 0 else 0
        else:
            slot_shares = whole_shares(budget, opened, schedule, entry_side)
        if slot_shares < 1:
            fills.append({
                "ticker": pick.ticker, "side": entry_side, "shares": 0,
                "price": opened, "fee": Decimal("0"), "reason": "no whole share",
                "date": date,
            })
            continue
        affordable = whole_shares(account.cash, opened, schedule, entry_side)
        shares = min(slot_shares, affordable)
        if shares < 1:
            # A flat account that cannot buy one share is "no whole share".
            # "no cash" is only when held lots leave too little cash.
            equity_shares = whole_shares(equity, opened, schedule, entry_side)
            if mode == "own_shares" and equity_shares < 1:
                fills.append({
                    "ticker": pick.ticker, "side": entry_side, "shares": 0,
                    "price": opened, "fee": Decimal("0"), "reason": "no whole share",
                    "date": date,
                })
                continue
            skipped = {
                "ticker": pick.ticker, "side": entry_side, "shares": 0,
                "price": opened, "fee": Decimal("0"), "reason": "no cash",
                "date": date,
            }
            if mode == "slot":
                skipped["mark"] = "plan sets no priority"
            fills.append(skipped)
            continue
        fee = fee_for(schedule, entry_side, shares, opened)
        if pick.side == "long":
            account.cash -= opened * Decimal(shares) + fee
        else:
            account.cash -= fee
        account.fees += fee
        target = None
        if pick.target_pct is not None:
            sign = Decimal("1") if pick.side == "long" else Decimal("-1")
            target = opened * (Decimal("1") + sign * pick.target_pct)
        exit_index = None
        # A dated close cover (Theme Radar shorts) is not the next-open hold.
        if pick.hold_sessions is not None and not pick.exit_on:
            exit_index = session_index + pick.hold_sessions
        account.lots.append(Lot(
            ticker=pick.ticker, side=pick.side, shares=shares, entry_px=opened,
            entry_date=date, stop=pick.stop, target=target, exit_index=exit_index,
            exit_on=pick.exit_on,
        ))
        fills.append({
            "ticker": pick.ticker, "side": entry_side, "shares": shares,
            "price": opened, "fee": fee, "reason": "open", "date": date,
            "position": pick.side,
        })

    still: list[Lot] = []
    for lot in account.lots:
        bar = bars.get(lot.ticker)
        high = _bar_px(bar, "high")
        low = _bar_px(bar, "low")
        opened = _bar_px(bar, "open")
        if high is None or low is None or opened is None:
            still.append(lot)
            continue
        stop_hit = target_hit = False
        if lot.side == "long":
            stop_hit = lot.stop is not None and low <= lot.stop
            target_hit = lot.target is not None and high >= lot.target
        else:
            stop_hit = lot.stop is not None and high >= lot.stop
            target_hit = lot.target is not None and low <= lot.target
        if stop_hit:
            fill = _book_exit(lot, lot.stop, schedule, "stop", date)
            apply_cash_exit(account, lot, fill)
            fills.append(fill)
        elif target_hit and lot.target is not None:
            price = lot.target
            if lot.side == "long" and opened >= lot.target:
                price = opened
            if lot.side == "short" and opened <= lot.target:
                price = opened
            fill = _book_exit(lot, price, schedule, "target", date)
            apply_cash_exit(account, lot, fill)
            fills.append(fill)
        else:
            still.append(lot)
    account.lots = still
    return fills


def mark_equity(account: Account, bars: dict[str, dict]) -> tuple[Decimal, list[str]]:
    """Mark at the session close. A missing print stays at the entry price."""
    equity = account.cash
    missing = []
    for lot in account.lots:
        bar = bars.get(lot.ticker) or {}
        px = bar.get("close")
        if px is None:
            px = bar.get("open")
        if px is None:
            missing.append(lot.ticker)
            px = lot.entry_px
        else:
            px = Decimal(str(px))
        if lot.side == "long":
            equity += px * Decimal(lot.shares)
        else:
            equity += (lot.entry_px - px) * Decimal(lot.shares)
    return equity, missing


def load_mark_notes(path: Path | None = None) -> dict[tuple[str, str], str]:
    """Add-only notes keyed by book and date. The first line for a key wins.

    These lines are not written into days.jsonl. A sealed row stays as sealed.
    A later ``close_equity`` line does not replace this note.
    """
    dest = path or MARK_NOTES_PATH
    notes: dict[tuple[str, str], str] = {}
    if not dest.is_file():
        return notes
    for line in dest.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        notes.setdefault((str(row["name"]), str(row["date"])), str(row["note"]))
    return notes


def mark_note_lines(path: Path | None = None) -> list[str]:
    dest = path or MARK_NOTES_PATH
    if not dest.is_file():
        return []
    return [line for line in dest.read_text(encoding="utf-8").splitlines() if line.strip()]


def assert_mark_notes_append_only(base_text: str, head_text: str) -> None:
    """Existing mark-note lines are a prefix. A run may only append."""
    base = [line for line in base_text.splitlines() if line.strip()]
    head = [line for line in head_text.splitlines() if line.strip()]
    if head[: len(base)] != base:
        raise ValueError(
            "mark notes are add-only: an existing line was edited, removed, or reordered"
        )


def load_close_equities(path: Path | None = None) -> dict[tuple[str, str], str]:
    """First close-equity line for each book and date. Later lines do not replace it."""
    found: dict[tuple[str, str], str] = {}
    for line in mark_note_lines(path):
        row = json.loads(line)
        if row.get("kind") != CLOSE_EQUITY_KIND:
            continue
        found.setdefault((str(row["name"]), str(row["date"])), str(row["close_equity"]))
    return found


def append_mark_note(row: dict, path: Path | None = None) -> None:
    """Append one JSON line. The bytes already in the file stay as they are."""
    dest = path or MARK_NOTES_PATH
    dest.parent.mkdir(parents=True, exist_ok=True)
    prior = dest.read_text(encoding="utf-8") if dest.is_file() else ""
    if prior and not prior.endswith("\n"):
        raise ValueError("mark notes are add-only: the file has no trailing newline")
    line = json.dumps(row, separators=(",", ":"), ensure_ascii=False)
    head = prior + line + "\n"
    assert_mark_notes_append_only(prior, head)
    dest.write_text(head, encoding="utf-8")


def close_source_label(day: str) -> str:
    return f"Yahoo split-adjusted {day} daily close"


def require_close_mark(row: dict, bars: dict[str, dict]) -> Decimal:
    """Close equity from cash and lots. A missing close is an error, not a guess."""
    equity = close_mark_equity(row, bars)
    if equity is None:
        held = [str(pos.get("ticker") or "") for pos in (row.get("positions") or [])]
        raise ValueError(
            f"close missing for {row.get('name')} {row.get('date')}: {','.join(held)}. "
            "Not guessing a close."
        )
    return equity


def session_closes(tickers: set[str], day: str) -> dict[str, dict]:
    """Bars for ``day`` from the sim price store, then Yahoo for any name still missing.

    A ticker with no close is omitted. Callers that must not guess use
    ``require_close_mark``, which fails when a held name is absent.
    """
    store = load_bars(tickers, {day})
    missing = sorted(
        ticker for ticker in tickers
        if (store.get((ticker, day)) or {}).get("close") is None
    )
    live = fetch_live_bars(missing, day) if missing else {}
    out: dict[str, dict] = {}
    for ticker in tickers:
        bar = store.get((ticker, day))
        if bar is None or bar.get("close") is None:
            bar = live.get((ticker, day))
        if bar is None or bar.get("close") is None:
            continue
        out[ticker] = bar
    return out


def close_equity_note(row: dict, equity: Decimal) -> dict:
    day = str(row["date"])
    return {
        "date": day,
        "name": str(row["name"]),
        "kind": CLOSE_EQUITY_KIND,
        "close_equity": q(equity),
        "sealed_equity": str(row["equity"]),
        "close_source": close_source_label(day),
        "note": CLOSE_EQUITY_NOTE,
    }


def append_close_equity_note(row: dict, equity: Decimal, path: Path | None = None) -> None:
    """One close line per book and date. A second line is refused."""
    key = (str(row["name"]), str(row["date"]))
    already = load_close_equities(path)
    if key in already:
        if already[key] != q(equity):
            raise ValueError(
                f"mark notes are add-only: {key[0]} {key[1]} close equity "
                f"is already {already[key]}"
            )
        return
    append_mark_note(close_equity_note(row, equity), path)


def day_pct_text(start: Decimal | str, close: Decimal | str) -> str:
    """Percent from the book's start cash to the close equity."""
    start_d = Decimal(str(start))
    close_d = Decimal(str(close))
    if start_d <= 0:
        raise ValueError("day percent needs a positive start equity")
    pct = ((close_d - start_d) / start_d * Decimal(100)).quantize(
        Decimal("0.01"), rounding=ROUND_HALF_UP,
    )
    return format(pct, "+.2f") + "%"


def close_mark_equity(row: dict, bars: dict[str, dict]) -> Decimal | None:
    """Cash and lots marked at the close. The stored equity is not an input.

    Returns None when a held name has no close, so an intraday last is not
    labeled as the close by mistake.
    """
    account = Account(
        cash=Decimal(str(row["cash"])),
        fees=Decimal(str(row.get("fees") or "0")),
    )
    for pos in row.get("positions") or []:
        ticker = str(pos["ticker"])
        bar = bars.get(ticker) or {}
        if bar.get("close") is None:
            return None
        account.lots.append(Lot(
            ticker=ticker,
            side=str(pos["side"]),
            shares=int(pos["shares"]),
            entry_px=Decimal(str(pos["entry_px"])),
            entry_date=str(pos.get("entry_date") or row.get("date") or ""),
        ))
    equity, missing = mark_equity(account, bars)
    if missing:
        return None
    return equity


def in_write_freeze(now: datetime) -> bool:
    local = now.astimezone(ET)
    start = local.replace(hour=3, minute=0, second=0, microsecond=0)
    end = local.replace(hour=9, minute=40, second=0, microsecond=0)
    return start <= local < end


def session_open(day: str) -> datetime:
    return datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=ET)


def session_close(day: str) -> datetime:
    return datetime.combine(date.fromisoformat(day), time(16, 0), tzinfo=ET)


def before_open(when: datetime, day: str) -> bool:
    return when.astimezone(ET) < session_open(day)


def close_is_final(now: datetime, day: str) -> bool:
    """The daily close is the 16:00 ET print, not the mid-session last."""
    return now.astimezone(ET) >= session_close(day)


def is_final_run(now: datetime, day: str) -> bool:
    """The run that decides a still-open row.

    Under the current cron that is 16:15 ET (20:15 UTC). A dispatch before
    the close keeps retrying a missing open.
    """
    return close_is_final(now, day)


def add_trading_days(day: str, n: int) -> str:
    """``n`` NYSE sessions after ``day``. Weekends and full-day holidays skip."""
    from .skip_if_good import _next_weekday
    cursor = day
    for _ in range(max(0, n)):
        cursor = _next_weekday(cursor)
    return cursor


def cover_at_close(account: Account, bars: dict[str, dict], schedule: Schedule,
                   day: str, now: datetime) -> list[dict]:
    """Cover shorts whose last hold day is ``day``, at that day's close.

    Other books do not set ``exit_on``, so this does not touch them.
    Before 16:00 ET the close is not the session close, and the lot stays.
    """
    if not close_is_final(now, day):
        return []
    fills: list[dict] = []
    kept: list[Lot] = []
    for lot in account.lots:
        if lot.exit_on != day or lot.side != "short":
            kept.append(lot)
            continue
        price = _bar_px(bars.get(lot.ticker), "close")
        if price is None:
            kept.append(lot)
            continue
        fill = _book_exit(lot, price, schedule, "hold", day)
        apply_cash_exit(account, lot, fill)
        fills.append(fill)
    account.lots = kept
    return fills


def clock_et(when: datetime) -> str:
    return when.astimezone(ET).strftime("%H:%M")


def next_weekday(day: str) -> str:
    cursor = date.fromisoformat(day) + timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor.isoformat()


def prior_weekday(day: str) -> str:
    """Calendar weekday before ``day``. Saturday and Sunday are skipped.

    This is the inverse of ``next_weekday``: a Monday session reads Friday's file.
    """
    cursor = date.fromisoformat(day) - timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor -= timedelta(days=1)
    return cursor.isoformat()


def nyse_sessions_through(start: str, end: str) -> list[str]:
    """NYSE sessions from ``start`` through ``end``, inclusive. Later days are omitted."""
    if not start or not end or end < start:
        return []
    from .skip_if_good import _session_date
    cursor = date.fromisoformat(start)
    last = date.fromisoformat(end)
    found = []
    while cursor <= last:
        if _session_date(cursor):
            found.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return found


def book_name(strategy: str) -> str:
    stem = strategy if strategy.endswith("_webull_sim") else f"{strategy}_webull_sim"
    return stem


def is_theme_book(name: str) -> bool:
    return name.startswith("theme_radar_") and name.endswith("_webull_sim")


def canon(row: dict) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def _num(fill: dict) -> dict:
    out = dict(fill)
    for key in ("price", "fee"):
        if isinstance(out.get(key), Decimal):
            out[key] = q(out[key])
    return out


def lot_snapshot(account: Account) -> list[dict]:
    rows = []
    for lot in account.lots:
        rows.append({
            "ticker": lot.ticker,
            "side": lot.side,
            "shares": lot.shares,
            "entry_px": q(lot.entry_px),
            "entry_date": lot.entry_date,
        })
    return rows


def make_row(
    *,
    name: str,
    day: str,
    source: str,
    commit: str,
    commit_et: str,
    reason: str,
    note: str,
    section: str,
    locked_trade: bool,
    final: bool,
    cash: Decimal,
    fees: Decimal,
    equity: Decimal | None,
    fills: list[dict],
    positions: list[dict],
    picks: int,
    borrow: str = "",
    sandbox: str = "",
    sizing: str = "",
) -> dict:
    return {
        "name": name,
        "date": day,
        "source": source,
        "commit": commit,
        "commit_et": commit_et,
        "reason": reason,
        "note": note,
        "section": section,
        "locked_trade": locked_trade,
        "final": final,
        "cash": q(cash),
        "fees": q(fees),
        "equity": "" if equity is None else q(equity),
        "fills": [_num(f) for f in fills],
        "positions": positions,
        "picks": picks,
        "borrow": borrow,
        "sandbox": sandbox,
        "sizing": sizing,
        "start_cash": q(START_CASH),
    }


def classify(day: str, before: bool, commit_at: datetime | None,
             picks: list, exits: list[str]) -> tuple[str, bool]:
    """Return (reason, tradable). A tradable plan may still lack an open."""
    if commit_at is None:
        return "no pre-09:30 plan", False
    if not before:
        local = commit_at.astimezone(ET)
        if local.date().isoformat() == day:
            stamp = local.strftime("%H:%M")
        else:
            stamp = local.strftime("%Y-%m-%d %H:%M")
        return f"plan committed {stamp} ET", False
    if not picks and not exits:
        return "sat out, 0 picks", False
    return "", True


def section_for(day: str, locked_trade: bool) -> str:
    if day < FIRST_LOCKED:
        return "built_after"
    if locked_trade:
        return "locked"
    return "not_a_locked_trade"


def final_row(day: str, now: datetime, tradable: bool, opens_ok: bool) -> bool:
    """A row stays a draft until that session's 16:00 ET close.

    Open fills can show earlier. They are not final, and they are not sealed,
    until the close run. A day whose close is already in the past can be final.
    """
    if now.astimezone(ET) < session_close(day):
        return False
    if day < FIRST_LOCKED:
        return (not tradable) or opens_ok
    if not tradable:
        return True
    return opens_ok


def _restore_account(row: dict, plan: dict, session_index: int, previous: Account) -> Account:
    """End state of a final row. Stops and targets come from the plan, not a rescore."""
    account = Account(
        cash=Decimal(str(row["cash"])),
        fees=Decimal(str(row.get("fees") or "0")),
    )
    picks = {pick.ticker: pick for pick in plan.get("picks") or []}
    carried = {(lot.ticker, lot.entry_date): lot for lot in previous.lots}
    for pos in row.get("positions") or []:
        ticker = str(pos["ticker"])
        entry = Decimal(str(pos["entry_px"]))
        side = str(pos["side"])
        old = carried.get((ticker, pos.get("entry_date")))
        stop = old.stop if old is not None else None
        target = old.target if old is not None else None
        exit_index = old.exit_index if old is not None else None
        exit_on = old.exit_on if old is not None else None
        pick = picks.get(ticker)
        if pick is not None and pos.get("entry_date") == plan["date"]:
            stop = pick.stop
            target = None
            if pick.target_pct is not None:
                sign = Decimal("1") if side == "long" else Decimal("-1")
                target = entry * (Decimal("1") + sign * pick.target_pct)
            exit_on = pick.exit_on
            exit_index = None
            if pick.hold_sessions is not None and not pick.exit_on:
                exit_index = session_index + pick.hold_sessions
        account.lots.append(Lot(
            ticker=ticker, side=side, shares=int(pos["shares"]),
            entry_px=entry, entry_date=str(pos["entry_date"]),
            stop=stop, target=target, exit_index=exit_index, exit_on=exit_on,
        ))
    return account


def run_book(
    name: str,
    plans: list[dict],
    sessions: list[str],
    bars_for,
    schedule: Schedule,
    now: datetime,
    sandbox_for=None,
    sealed_rows: dict[str, dict] | None = None,
    close_marks: dict[tuple[str, str], str] | None = None,
) -> list[dict]:
    """Sequential sim. Locked cash starts over at the first locked day.

    A row already marked final in days.jsonl is copied through. It is not
    rebuilt, so a later close mark cannot change its bytes.
    """
    index = {day: i for i, day in enumerate(sessions)}
    built = Account()
    locked = Account()
    rows = []
    sealed_rows = sealed_rows or {}
    noted = load_mark_notes() if close_marks is not None else {}
    for plan in plans:
        day = plan["date"]
        account = built if day < FIRST_LOCKED else locked
        prior = sealed_rows.get(day)
        if prior and prior.get("final"):
            rows.append(prior)
            restored = _restore_account(prior, plan, index.get(day, 0), account)
            if day < FIRST_LOCKED:
                built = restored
            else:
                locked = restored
            if close_marks is not None and close_is_final(now, day) and (name, day) in noted:
                tickers = [lot.ticker for lot in restored.lots]
                marked = close_mark_equity(prior, bars_for(day, tickers))
                if marked is not None:
                    close_marks[(name, day)] = q(marked)
            continue
        tradable, reason = plan["tradable"], plan["reason"]
        note = plan.get("note") or ""
        # A day before the lock can still be simulated once. The late commit
        # stays visible, and the result stays out of the locked section.
        # A row that was not a pre-09:30 plan is not force-traded.
        if (
            day < FIRST_LOCKED
            and plan["picks"]
            and not tradable
            and reason != "no pre-09:30 plan"
        ):
            note = (note + " " + reason).strip()
            reason = ""
            tradable = True
        tickers = [pick.ticker for pick in plan["picks"]]
        tickers += [lot.ticker for lot in account.lots]
        tickers += list(plan["exits"])
        bars = bars_for(day, tickers)
        missing_picks = []
        missing_exits = []
        if tradable:
            for pick in plan["picks"]:
                bar = bars.get(pick.ticker)
                if not bar or bar.get("open") is None:
                    missing_picks.append(pick.ticker)
            for ticker in plan["exits"]:
                if any(lot.ticker == ticker for lot in account.lots):
                    bar = bars.get(ticker)
                    if not bar or bar.get("open") is None:
                        missing_exits.append(ticker)
        opens_ok = not missing_picks and not missing_exits
        # Only a row that is still open can take this. A locked row is not
        # rewritten; write_books keeps the sealed line.
        accept_missing = (
            day >= FIRST_LOCKED
            and not opens_ok
            and is_final_run(now, day)
        )
        fills: list[dict] = []
        mode = resolve_sizing(plan["picks"], plan.get("sizing") or "slot")
        # A sit-out or a day with no pre-09:30 plan buys nothing. From
        # SIT_OUT_FROM it still runs the open exit rules on lots already held,
        # then marks at the close. Theme books keep cover_at_close below.
        exit_held = (
            not tradable
            and day >= SIT_OUT_FROM
            and not is_theme_book(name)
            and reason in ("no pre-09:30 plan", "sat out, 0 picks")
        )
        if tradable:
            # One missing print does not drop the other picks. Each name is
            # sized off the same open equity. Before the final run the miss
            # stays "open not observed" and the row stays unlocked.
            fills = apply_session(
                account, plan["picks"], plan["exits"], bars, schedule,
                day, index[day], sizing=mode,
            )
            if accept_missing:
                for fill in fills:
                    if fill.get("reason") == "open not observed":
                        fill["reason"] = "no open, not filled"
                for ticker in missing_exits:
                    lot = next(lot for lot in account.lots if lot.ticker == ticker)
                    fills.append({
                        "ticker": ticker,
                        "side": "sell" if lot.side == "long" else "buy",
                        "shares": 0,
                        "price": None,
                        "fee": Decimal("0"),
                        "reason": "no open, not filled",
                        "date": day,
                    })
                names = list(dict.fromkeys(missing_picks + missing_exits))
                note = (note + " no open, not filled: " + ",".join(names)).strip()
                opens_ok = True
            elif not opens_ok:
                traded = any(fill.get("shares") for fill in fills)
                if not traded:
                    reason = "open not observed"
                else:
                    if missing_picks:
                        note = (note + " open not observed: " + ",".join(missing_picks)).strip()
        elif exit_held:
            fills = apply_session(
                account, [], plan["exits"], bars, schedule,
                day, index[day], sizing=mode,
            )
        if is_theme_book(name):
            # A late or empty day still covers shorts that were already on.
            # New picks were not opened above unless the plan was tradable.
            fills = list(fills) + cover_at_close(account, bars, schedule, day, now)
        equity, missing_marks = mark_equity(account, bars)
        if missing_marks:
            note = (note + " mark not observed, carried at entry: " + ",".join(missing_marks)).strip()
        locked_trade = bool(
            day >= FIRST_LOCKED and tradable and opens_ok and reason == ""
        )
        # final_row stays false until 16:00 ET, so a market-hours row is a
        # draft: fills can show, and the line stays rewritable.
        is_final = final_row(day, now, tradable, opens_ok)
        if any(lot.exit_on == day for lot in account.lots):
            is_final = False
        sandbox = ""
        if sandbox_for and name == "h1_webull_sim":
            sandbox = sandbox_for(day)
        rows.append(make_row(
            name=name,
            day=day,
            source=plan.get("source") or "",
            commit=plan.get("commit") or "",
            commit_et=plan.get("commit_et") or "",
            reason=reason,
            note=note,
            section=section_for(day, locked_trade),
            locked_trade=locked_trade,
            final=is_final,
            cash=account.cash,
            fees=account.fees,
            equity=equity,
            fills=fills,
            positions=lot_snapshot(account),
            picks=len(plan["picks"]),
            borrow=plan.get("borrow") or "",
            sandbox=sandbox,
            sizing=sizing_label(mode, len(plan["picks"])),
        ))
    return rows


def sandbox_label(payload: dict | None) -> str:
    """Read-only. Never places or cancels an order."""
    if not payload:
        return "not observed"
    if payload.get("fill_status") in (None, "", "not_observed"):
        return "not observed"
    sent = payload.get("sent") or []
    if not sent:
        return "not observed"
    gaps = []
    for order in sent:
        fill = order.get("avg_price", order.get("fill_price", order.get("filled_price")))
        if fill is None:
            return "not observed"
        gaps.append({
            "ticker": order.get("ticker"),
            "side": order.get("side"),
            "shares": order.get("shares"),
            "fill_price": fill,
        })
    return json.dumps(gaps, sort_keys=True)


def git_commits(path: str) -> list[tuple[str, datetime]]:
    proc = subprocess.run(
        ["git", "log", "--format=%H%x09%cI", "--", path],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    rows = []
    for line in proc.stdout.splitlines():
        sha, iso = line.split("\t", 1)
        rows.append((sha, datetime.fromisoformat(iso)))
    return rows


def git_show(sha: str, path: str) -> str:
    proc = subprocess.run(
        ["git", "show", f"{sha}:{path}"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    if proc.returncode != 0:
        raise FileNotFoundError(path)
    return proc.stdout


def proving_commit(commits: list[tuple[str, datetime]], day: str):
    """Latest commit strictly before 09:30 ET, else the newest commit."""
    chosen = None
    for sha, when in commits:
        if before_open(when, day):
            chosen = (sha, when, True)
            break
    if chosen:
        return chosen
    if not commits:
        return None
    sha, when = commits[0]
    return sha, when, False


def _plan(day, source, commit, when, before, picks, exits, note="", borrow=""):
    reason, tradable = classify(day, before, when, picks, exits)
    commit_et = ""
    if when is not None:
        commit_et = when.astimezone(ET).isoformat(timespec="seconds")
    return {
        "date": day,
        "source": source,
        "commit": commit or "",
        "commit_et": commit_et,
        "before": before,
        "picks": picks,
        "exits": exits,
        "reason": reason,
        "tradable": tradable,
        "note": note,
        "borrow": borrow,
    }


def empty_plan(day: str, reason: str, note: str = "") -> dict:
    return _plan(day, "", "", None, False, [], [], note=note or reason)


def parse_excel(text: str) -> dict[str, list[Pick]]:
    out: dict[str, list[Pick]] = {}
    started = False
    for line in text.splitlines():
        if line.startswith("| ticker |"):
            started = True
            continue
        if not started:
            continue
        if not line.startswith("|"):
            break
        if set(line.replace("|", "").strip()) <= {"-", " "}:
            continue
        cols = [part.strip() for part in line.strip().strip("|").split("|")]
        if len(cols) < 4:
            continue
        ticker, side, strategy, exit_name = cols[0], cols[1].lower(), cols[2], cols[3]
        pick = Pick(ticker=ticker.upper(), side="long" if side == "long" else "short")
        if exit_name.startswith("tp") and exit_name[2:].isdigit():
            pick.target_pct = Decimal(exit_name[2:]) / Decimal("100")
        elif exit_name.startswith("hold") and exit_name[4:].isdigit():
            pick.hold_sessions = int(exit_name[4:])
        out.setdefault(strategy, []).append(pick)
    return out


def excel_universe() -> list[str]:
    if not EXCEL_STRATS.is_dir():
        return []
    return sorted(p.name for p in EXCEL_STRATS.iterdir() if (p / "card.json").is_file())


def load_excel_plans(today: str | None = None) -> dict[str, list[dict]]:
    books: dict[str, list[dict]] = {name: [] for name in excel_universe()}
    files = sorted(EXCEL_DIR.glob("*_excel_bot.md"))
    for path in files:
        if "draft" in path.name:
            continue
        file_day = path.name[:10]
        session = next_weekday(file_day)
        rel = path.relative_to(ROOT).as_posix()
        commits = git_commits(rel)
        proved = proving_commit(commits, session)
        text = path.read_text(encoding="utf-8")
        if proved:
            sha, when, before = proved
            # A commit after the session open is not the sealed plan.
            if not before:
                for name in books:
                    books[name].append(_plan(
                        session, rel, sha, when, False, [], [],
                        note="final file only; drafts are ignored",
                    ))
                continue
            # Use the tree as of that commit when it differs from HEAD.
            try:
                text = git_show(sha, rel)
            except FileNotFoundError:
                pass
        grouped = parse_excel(text) if proved and proved[2] else {}
        note = (
            "Sim buys the next 09:30 open. The research card buys the "
            "signal-day close and is not a comparable result."
        )
        for name in books:
            picks = grouped.get(name, [])
            if not proved:
                books[name].append(empty_plan(session, "no pre-09:30 plan", note))
                continue
            sha, when, before = proved
            books[name].append(_plan(
                session, rel, sha, when, before, picks, [], note=note,
            ))
    _fill_missing_excel_finals(books, today)
    for name in books:
        books[name].sort(key=lambda plan: plan["date"])
    return {book_name(k): v for k, v in books.items()}


def _fill_missing_excel_finals(books: dict[str, list[dict]], today: str | None) -> None:
    """Empty plan when the prior weekday has no excel_bot final.

    A draft is not a final. Its picks are not read. Sessions before
    SIT_OUT_FROM stay as they were, and a session after ``today`` is not written.
    """
    if not books:
        return
    if today is None:
        today = datetime.now(ET).date().isoformat()
    scheduled = {plan["date"] for plans in books.values() for plan in plans}
    for session in nyse_sessions_through(SIT_OUT_FROM, today):
        if session in scheduled:
            continue
        signal = prior_weekday(session)
        if (EXCEL_DIR / f"{signal}_excel_bot.md").is_file():
            continue
        note = f"sat out: no excel_bot final for {signal}"
        for name in books:
            books[name].append(empty_plan(session, "no pre-09:30 plan", note))
        scheduled.add(session)


def load_ticket_plans() -> dict[str, list[dict]]:
    books: dict[str, list[dict]] = {}
    files = sorted(TICKET_DIR.glob("*_strategy_tickets.json"))
    for path in files:
        day = path.name[:10]
        rel = path.relative_to(ROOT).as_posix()
        proved = proving_commit(git_commits(rel), day)
        if not proved:
            continue
        sha, when, before = proved
        text = git_show(sha, rel)
        payload = json.loads(text)
        strategies = payload.get("strategies") or {}
        for name, rec in strategies.items():
            if str(name).startswith("excel_"):
                # Excel sleeves come from the dated final file, not this sheet.
                continue
            buys = rec.get("buy") or []
            sells = rec.get("sell") or []
            picks = []
            for buy in buys:
                side = str(buy.get("side") or "long").lower()
                hold = _hold_from_name(name)
                picks.append(Pick(
                    ticker=str(buy["ticker"]).upper(),
                    side="short" if side == "short" else "long",
                    hold_sessions=hold,
                ))
            exits = [str(row["ticker"]).upper() for row in sells if row.get("ticker")]
            plan = _plan(day, rel, sha, when, before, picks, exits)
            books.setdefault(book_name(name), []).append(plan)
    return books


def _hold_from_name(name: str) -> int:
    token = name.lower()
    if token.endswith("_h1") or token.endswith("_1d") or "1d" in token.split("_"):
        return 1
    if token.endswith("_h3") or "_3d" in f"_{token}_":
        return 3
    if token.endswith("_h5") or "_1w" in f"_{token}_":
        return 5
    if "_2w" in f"_{token}_":
        return 10
    if "_1m" in f"_{token}_":
        return 21
    return 1


def load_h1_plans() -> list[dict]:
    if not H1_LOG.is_file():
        return []
    rel = H1_LOG.relative_to(ROOT).as_posix()
    plans = []
    for line in H1_LOG.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("kind") != "plan":
            continue
        day = str(row["date"])
        stamp = str(row.get("committed_at") or "")
        when = datetime.fromisoformat(stamp.replace("Z", "+00:00")) if stamp else None
        before = bool(when and before_open(when, day))
        proc = subprocess.run(
            ["git", "log", "-S", stamp, "--format=%H", "-1", "--", rel],
            cwd=ROOT, check=False, capture_output=True, text=True,
        ) if stamp else None
        sha = (proc.stdout.strip().splitlines() or [""])[0] if proc else ""
        picks = [
            Pick(ticker=str(p["ticker"]).upper(), side="long", hold_sessions=1)
            for p in (row.get("picks") or [])
        ]
        exits = [str(s["ticker"]).upper() for s in (row.get("planned_sells") or [])]
        note = (
            "Sealed h1 plan log. The plan sets its own count and equal weight. "
            "Sim cash starts at $10,000 and is not the research book's cash."
        )
        if row.get("holdup_on"):
            note += " holdup_on."
        plan = _plan(day, rel, sha, when, before, picks, exits, note=note)
        plan["sizing"] = "own_count"
        plans.append(plan)
    return plans


def load_shadow_plans() -> dict[str, list[dict]]:
    books: dict[str, list[dict]] = {}
    for path in sorted(SHADOW_DIR.glob("*.picks.json")):
        day = path.name[:10]
        rel = path.relative_to(ROOT).as_posix()
        proved = proving_commit(git_commits(rel), day)
        payload = json.loads(path.read_text(encoding="utf-8"))
        recipes = payload.get("recipes") or {}
        for name, rec in recipes.items():
            raw_picks = rec.get("picks") or []
            picks = [
                Pick(ticker=str(p.get("ticker") or p).upper(), side="long", hold_sessions=1)
                if not isinstance(p, dict) else
                Pick(ticker=str(p["ticker"]).upper(), side="long", hold_sessions=1)
                for p in raw_picks
            ]
            if not proved:
                plan = empty_plan(day, "no pre-09:30 plan")
            else:
                sha, when, before = proved
                note = "forward_shadow_v1 picks file"
                if payload.get("sat_out"):
                    note += " · " + str(payload.get("reason") or "sat out")
                plan = _plan(day, rel, sha, when, before, picks, [], note=note)
            books.setdefault(book_name(f"forward_shadow_{name}"), []).append(plan)
    return books


THEME_SOURCE = "SRoyaltyy/theme-radar:research/shadow_log/log.csv"
THEME_PLAN_DIR = "research/shadow_log/plans"
THEME_CELLS = (
    "fpe_delta_t3_earn_today_3d",
    "fresh_dcp_t1_ep_ge03_2d",
    "fresh_dcp_t1_avoid_ah_3d",
)
THEME_BORROW_NOTE = "Borrow cost and availability are not modeled in this sim."
THEME_LOG_NOTE = (
    "Theme Radar's shadow-log figures include an assumed 0.3% borrow plus a 15bp fee "
    "on every short (its own assumption, not a real borrow rate), so the log will read "
    "somewhat worse than this sim for the same trades."
)


def _borrow_label(row: dict) -> str:
    for name in BORROW_FIELDS:
        if row.get(name):
            return f"log field {name}={row[name]}"
    return "borrow not modeled"


def _theme_pick(row: dict) -> Pick | None:
    ticker = (row.get("ticker") or "").upper()
    if not ticker:
        return None
    try:
        hold = int(row.get("hold_days") or "0")
    except ValueError:
        hold = 0
    return Pick(ticker=ticker, side="short", hold_sessions=hold or None)


def group_theme_rows(rows: list[dict]) -> dict[str, list[dict]]:
    """Legacy shadow-log grouping. The sim does not call this.

    ``load_theme_plans`` reads ``plans/plan_<entry>.csv`` instead. This
    remains for the log-shaped fixtures: a row whose first commit is at or
    after 09:30 ET on its entry day is not a fill, and the day still gets a
    visible row, reason ``no pre-09:30 plan``.
    """
    grouped: dict[tuple[str, str], list[dict]] = {}
    for row in rows:
        cell = row.get("cell") or "theme_radar"
        day = row.get("entry") or row.get("date") or ""
        if not day or not (row.get("ticker") or "").strip():
            continue
        grouped.setdefault((cell, day), []).append(row)
    books: dict[str, list[dict]] = {}
    for (cell, day), day_rows in sorted(grouped.items()):
        picks = []
        early = []
        late = []
        for row in day_rows:
            pick = _theme_pick(row)
            if pick is None:
                continue
            when = row.get("_when")
            if isinstance(when, datetime) and before_open(when, day):
                early.append(row)
                picks.append(pick)
            else:
                late.append(row)
        borrow = "borrow not modeled"
        for row in day_rows:
            label = _borrow_label(row)
            if label != "borrow not modeled":
                borrow = label
                break
        name = book_name(f"theme_radar_{cell}")
        if early and not late:
            when = max(row["_when"] for row in early)
            sha = next(row["_sha"] for row in early if row["_when"] == when)
            note = (
                "Research short. Off the real Webull paper account. "
                "Enters at the 09:30 open and covers at the open after the hold. "
                + borrow + "."
            )
            plan = _plan(
                day, THEME_SOURCE, sha, when, True, picks, [],
                note=note, borrow=borrow,
            )
        elif early and late:
            when = max(row["_when"] for row in early)
            sha = next(row["_sha"] for row in early if row["_when"] == when)
            late_names = ", ".join(_theme_pick(row).ticker for row in late if _theme_pick(row))
            note = (
                "Research short. Off the real Webull paper account. "
                "Enters at the 09:30 open and covers at the open after the hold. "
                f"{late_names} first appeared after 09:30 ET on {day} and are not filled. "
                + borrow + "."
            )
            plan = _plan(
                day, THEME_SOURCE, sha, when, True, picks, [],
                note=note, borrow=borrow,
            )
        else:
            stamped = [row for row in late if isinstance(row.get("_when"), datetime)]
            when = min((row["_when"] for row in stamped), default=None)
            sha = ""
            if when is not None:
                sha = next(row["_sha"] for row in stamped if row["_when"] == when)
            names = ", ".join(
                pick.ticker for row in day_rows if (pick := _theme_pick(row)) is not None
            )
            kept = [pick for row in day_rows if (pick := _theme_pick(row)) is not None]
            note = (
                "Research short. Off the real Webull paper account. "
                f"{names} first appeared after 09:30 ET on {day}. Not a fill. "
                + borrow + "."
            )
            plan = _plan(
                day, THEME_SOURCE, sha, when, False, kept, [],
                note=note, borrow=borrow,
            )
            plan["reason"] = "no pre-09:30 plan"
            plan["tradable"] = False
        books.setdefault(name, []).append(plan)
    return books


def parse_theme_log(text: str, commit: str, when: datetime | None) -> dict[str, list[dict]]:
    """Parse one log text. ``when`` is that text's first appearance."""
    reader = csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        stamped = dict(row)
        stamped["_sha"] = commit
        stamped["_when"] = when
        rows.append(stamped)
    return group_theme_rows(rows)


def github_api(path: str, accept: str = "") -> tuple[int, str]:
    """GET ``https://api.github.com/{path}`` with no Actions token.

    ``GITHUB_TOKEN`` on ubuntu-latest is this repo's token. theme-radar
    answers 404 to it. A public read goes out unauthenticated.
    """
    url = "https://api.github.com/" + path.lstrip("/")
    header = accept or "application/vnd.github+json"
    env = {
        key: value for key, value in os.environ.items()
        if key not in {"GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN"}
    }
    try:
        proc = subprocess.run(
            [
                "curl", "-sS", "-w", "\n%{http_code}",
                "-H", f"Accept: {header}",
                "-H", "User-Agent: fullscan-webull-sim",
                url,
            ],
            cwd=ROOT, check=False, capture_output=True, text=True,
            timeout=60, env=env,
        )
    except (OSError, subprocess.TimeoutExpired):
        return 1, ""
    raw = proc.stdout or ""
    body, sep, code = raw.rpartition("\n")
    if not sep:
        return proc.returncode or 1, raw
    try:
        status = int(code.strip())
    except ValueError:
        return proc.returncode or 1, raw
    return (0 if status < 400 else status), body


def theme_radar_root() -> Path | None:
    """Local theme-radar checkout. The workflow sets ``THEME_RADAR_DIR``."""
    raw = os.environ.get("THEME_RADAR_DIR", "").strip()
    if not raw:
        return None
    path = Path(raw)
    if (path / ".git").exists():
        return path
    return None


def _theme_git(root: Path, args: list[str]) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(
            ["git", *args], cwd=root, check=False, capture_output=True,
            text=True, timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def _github_json(path: str):
    code, text = github_api(path)
    if not text.strip():
        return code, None
    try:
        return code, json.loads(text)
    except json.JSONDecodeError:
        return code, None


def _not_found(payload) -> bool:
    return isinstance(payload, dict) and "Not Found" in str(payload.get("message") or "")


def _plan_day(name: str) -> str | None:
    if not (name.startswith("plan_") and name.endswith(".csv")):
        return None
    day = name[len("plan_"):-len(".csv")]
    try:
        date.fromisoformat(day)
    except ValueError:
        return None
    return day


def theme_plan_names() -> list[str] | None:
    """``plan_<entry>.csv`` filenames. None when the listing could not be read."""
    root = theme_radar_root()
    if root is not None:
        folder = root / THEME_PLAN_DIR
        if not folder.is_dir():
            return None
        return sorted(
            path.name for path in folder.glob("plan_*.csv") if _plan_day(path.name)
        )
    code, payload = _github_json(
        f"repos/SRoyaltyy/theme-radar/contents/{THEME_PLAN_DIR}"
    )
    if _not_found(payload):
        return []
    if code != 0 or not isinstance(payload, list):
        return None
    names = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        if item.get("type") not in (None, "file"):
            continue
        name = str(item.get("name") or "")
        if _plan_day(name):
            names.append(name)
    return sorted(names)


def theme_commits(path: str) -> list[tuple[str, datetime]] | None:
    """Committer times for ``path``, newest first.

    ``[]`` means the path was never committed. None means the history
    could not be read. The first commit is the oldest of this list.
    """
    root = theme_radar_root()
    if root is not None:
        proc = _theme_git(root, ["log", "--format=%H%x09%cI", "--", path])
        if proc is None or proc.returncode != 0:
            return None
        found: list[tuple[str, datetime]] = []
        for line in proc.stdout.splitlines():
            if "\t" not in line:
                continue
            sha, iso = line.split("\t", 1)
            found.append((sha, datetime.fromisoformat(iso)))
        return found
    found = []
    for page in range(1, 21):
        code, payload = _github_json(
            "repos/SRoyaltyy/theme-radar/commits?path="
            f"{path}&per_page=100&page={page}"
        )
        if _not_found(payload):
            return [] if page == 1 else found
        if code != 0 or payload is None:
            return None if page == 1 else found
        if isinstance(payload, dict):
            return None if page == 1 else found
        if not isinstance(payload, list) or not payload:
            break
        for item in payload:
            if not isinstance(item, dict):
                continue
            sha = str(item.get("sha") or "")
            raw = ((item.get("commit") or {}).get("committer") or {}).get("date") or ""
            if not sha or not raw:
                continue
            when = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            found.append((sha, when))
        if len(payload) < 100:
            break
    return found


def theme_plan_text(path: str, sha: str) -> str | None:
    root = theme_radar_root()
    if root is not None:
        proc = _theme_git(root, ["show", f"{sha}:{path}"])
        if proc is None or proc.returncode != 0 or not proc.stdout.strip():
            return None
        return proc.stdout
    code, text = github_api(
        f"repos/SRoyaltyy/theme-radar/contents/{path}?ref={sha}",
        accept="application/vnd.github.raw",
    )
    if code != 0 or not text.strip():
        return None
    if text.lstrip().startswith("{") and "Not Found" in text:
        return None
    return text


def parse_plan_csv(text: str) -> list[dict]:
    """Plan rows. ``#`` trailer lines are provenance, not picks."""
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        lines.append(line)
    if not lines:
        return []
    return list(csv.DictReader(io.StringIO("\n".join(lines) + "\n")))


def theme_source(path: str) -> str:
    return f"SRoyaltyy/theme-radar:{path}"


def _plan_pick(row: dict, day: str) -> Pick | None:
    ticker = (row.get("ticker") or "").upper().strip()
    if not ticker:
        return None
    try:
        hold = int(str(row.get("hold_days") or "0").strip() or "0")
    except ValueError:
        hold = 0
    exit_on = add_trading_days(day, hold) if hold > 0 else None
    return Pick(
        ticker=ticker, side="short",
        hold_sessions=hold or None, exit_on=exit_on,
    )


def _theme_plan(day: str, source: str, sha: str, when: datetime,
                before: bool, picks: list[Pick], *, no_fires: bool = False) -> dict:
    borrow = "borrow not modeled"
    if before and picks:
        note = (
            "Research short. Off the real Webull paper account. "
            "Enters at the 09:30 open and covers at the close of the last hold day. "
            + borrow + "."
        )
        return _plan(day, source, sha, when, True, picks, [], note=note, borrow=borrow)
    if before and not picks:
        note = (
            "Research short. Off the real Webull paper account. "
            "Pre-open plan fired nothing for this rule. " + borrow + "."
        )
        if no_fires:
            note += " status=no_fires."
        return _plan(day, source, sha, when, True, [], [], note=note, borrow=borrow)
    names = ", ".join(pick.ticker for pick in picks) if picks else "The plan file"
    note = (
        "Research short. Off the real Webull paper account. "
        f"{names} is on a plan whose first commit is not before 09:30 ET. Not a fill. "
        + borrow + "."
    )
    plan = _plan(day, source, sha, when, False, picks, [], note=note, borrow=borrow)
    plan["reason"] = "no pre-09:30 plan"
    plan["tradable"] = False
    return plan


def plans_from_plan_file(path: str, day: str) -> dict[str, dict] | None:
    """One plan per rule. None when the history or the file could not be read.

    The lock uses the oldest commit on ``path``, not a later rewrite.
    A missing file is ``no pre-09:30 plan`` with no commit.
    """
    history = theme_commits(path)
    if history is None:
        return None
    source = theme_source(path)
    if not history:
        return {
            book_name(f"theme_radar_{cell}"): empty_plan(
                day, "no pre-09:30 plan",
                "No theme-radar plan file for this entry day.",
            )
            for cell in THEME_CELLS
        }
    # Newest-first history: the smallest committer time is the first commit.
    # A tied time keeps the later list entry, which is the older revision.
    sha, when = min(enumerate(history), key=lambda item: (item[1][1], -item[0]))[1]
    before = before_open(when, day)
    text = theme_plan_text(path, sha)
    if text is None:
        return None
    grouped: dict[str, list[dict]] = {}
    order: list[str] = []
    for row in parse_plan_csv(text):
        entry = (row.get("entry_date") or "").strip()
        if entry and entry != day:
            continue
        cell = (row.get("cell") or "").strip()
        if not cell or not (row.get("ticker") or "").strip():
            continue
        if cell not in grouped:
            order.append(cell)
        grouped.setdefault(cell, []).append(row)
    cells = list(THEME_CELLS)
    for cell in order:
        if cell not in cells:
            cells.append(cell)
    no_fires = "status=no_fires" in text
    books: dict[str, dict] = {}
    for cell in cells:
        picks = []
        for row in grouped.get(cell, []):
            pick = _plan_pick(row, day)
            if pick is not None:
                picks.append(pick)
        books[book_name(f"theme_radar_{cell}")] = _theme_plan(
            day, source, sha, when, before, picks,
            no_fires=no_fires and not picks,
        )
    return books


def _unread_theme(day: str, source: str = "") -> dict:
    plan = empty_plan(day, "no pre-09:30 plan", "theme-radar plan not readable this run")
    if source:
        plan["source"] = source
    return plan


def load_theme_plans() -> dict[str, list[dict]]:
    """Pre-open plan files, one book per rule. The shadow log is not the source."""
    books: dict[str, list[dict]] = {
        book_name(f"theme_radar_{cell}"): [] for cell in THEME_CELLS
    }
    names = theme_plan_names()
    if names is None:
        for cell in THEME_CELLS:
            books[book_name(f"theme_radar_{cell}")].append(_unread_theme(FIRST_LOCKED))
        return books
    for name in names:
        day = _plan_day(name) or ""
        # Theme Radar sim history starts on the first locked day.
        if not day or day < FIRST_LOCKED:
            continue
        path = f"{THEME_PLAN_DIR}/{name}"
        built = plans_from_plan_file(path, day)
        if built is None:
            for cell in THEME_CELLS:
                books[book_name(f"theme_radar_{cell}")].append(
                    _unread_theme(day, theme_source(path))
                )
            continue
        for book, plan in built.items():
            books.setdefault(book, []).append(plan)
    for cell in THEME_CELLS:
        book = book_name(f"theme_radar_{cell}")
        plans = books.setdefault(book, [])
        if FIRST_LOCKED in {plan["date"] for plan in plans}:
            continue
        plans.append(empty_plan(
            FIRST_LOCKED, "no pre-09:30 plan",
            "No theme-radar plan file for this entry day.",
        ))
    return books


def _holding_plan(plan: dict, day: str) -> dict:
    return {
        "date": day,
        "source": plan.get("source") or "",
        "commit": plan.get("commit") or "",
        "commit_et": plan.get("commit_et") or "",
        "before": True,
        "picks": [],
        "exits": [],
        "reason": "",
        "tradable": True,
        "note": "Holding the short. Cover at the close of the last hold day.",
        "borrow": plan.get("borrow") or "borrow not modeled",
    }


def hold_continuations(plans: list[dict], today: str) -> list[dict]:
    """Visit each session through the cover day, without adding those dates
    to any other book's calendar. Days after ``today`` stay off the book.
    """
    from .skip_if_good import _next_weekday
    existing = {plan["date"] for plan in plans}
    extra = []
    for plan in plans:
        if not plan.get("tradable"):
            continue
        deadlines = [pick.exit_on for pick in plan["picks"] if pick.exit_on]
        if not deadlines:
            continue
        last = max(deadlines)
        cursor = plan["date"]
        while cursor < last and cursor < today:
            cursor = _next_weekday(cursor)
            if cursor > last or cursor > today:
                break
            if cursor in existing:
                continue
            existing.add(cursor)
            extra.append(_holding_plan(plan, cursor))
    if not extra:
        return list(plans)
    return sorted([*plans, *extra], key=lambda plan: plan["date"])


def static_gap_plans(days: list[str]) -> dict[str, list[dict]]:
    books = {}
    for strategy, reason, note in NO_MORNING_PLAN:
        books[book_name(strategy)] = [
            empty_plan(day, reason, note) for day in days
        ]
    return books


def load_bars(tickers: set[str], days: set[str]) -> dict[tuple[str, str], dict]:
    if not tickers or not days or not PRICE_PATH.is_file():
        return {}
    import pandas as pd
    frame = pd.read_parquet(PRICE_PATH, columns=["date", "ticker", "open", "high", "low", "close"])
    frame["ticker"] = frame["ticker"].astype(str).str.upper()
    frame = frame[frame["ticker"].isin(tickers)]
    frame["day"] = frame["date"].dt.strftime("%Y-%m-%d")
    frame = frame[frame["day"].isin(days)]
    out = {}
    for row in frame.itertuples(index=False):
        out[(row.ticker, row.day)] = {
            "open": None if row.open != row.open else float(row.open),
            "high": None if row.high != row.high else float(row.high),
            "low": None if row.low != row.low else float(row.low),
            "close": None if row.close != row.close else float(row.close),
        }
    return out


def live_fetch_allowed(now: datetime, day: str) -> bool:
    """The 09:30 print is not knowable before the open."""
    return now.astimezone(ET) >= session_open(day) + timedelta(minutes=5)


def _px(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number != number or number <= 0:
        return None
    return number


def _bar_day(value) -> str:
    import pandas as pd
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is not None:
        stamp = stamp.tz_convert(ET)
    return stamp.strftime("%Y-%m-%d")


def _split_explains(ratio: float, splits: list[float]) -> bool:
    """True when a Yahoo split factor, or their product, matches the jump."""
    usable = [split for split in splits if split > 0]

    def near(factor: float) -> bool:
        return factor > 0 and abs(ratio - factor) / factor <= JUMP_TOL

    if not usable:
        return False
    price = 1.0
    shares = 1.0
    for split in usable:
        price *= 1.0 / split
        shares *= split
    if near(price) or near(shares):
        return True
    return any(near(split) or near(1.0 / split) for split in usable)


def official_opens(flat, actions, day: str) -> dict[tuple[str, str], dict]:
    """Keep today's Yahoo open. Drop an unexplained 3x jump. Do not invent a price.

    ``flat`` is the long OHLC table from ``_flatten_yf``. Earlier rows in that
    table are only the previous close for the jump check.
    """
    if flat is None or getattr(flat, "empty", True):
        return {}
    work = flat.copy()
    work["day"] = work["date"].map(_bar_day)
    work["ticker"] = work["ticker"].astype(str).str.upper()
    split_on: dict[tuple[str, str], list[float]] = {}
    if actions is not None and not getattr(actions, "empty", True) and "split" in actions.columns:
        act = actions.copy()
        act["day"] = act["date"].map(_bar_day)
        act["ticker"] = act["ticker"].astype(str).str.upper()
        for row in act.itertuples(index=False):
            factor = _px(getattr(row, "split", None))
            if factor is None or abs(factor - 1.0) <= 1e-12:
                continue
            split_on.setdefault((row.ticker, row.day), []).append(factor)
    by_ticker: dict[str, list] = {}
    for row in work.itertuples(index=False):
        by_ticker.setdefault(row.ticker, []).append(row)
    out = {}
    for ticker, rows in by_ticker.items():
        rows = sorted(rows, key=lambda item: item.day)
        today = [row for row in rows if row.day == day]
        if not today:
            continue
        bar = today[-1]
        opened = _px(bar.open)
        if opened is None:
            continue
        earlier = [row for row in rows if row.day < day and _px(row.close) is not None]
        if earlier:
            prev = earlier[-1]
            ratio = opened / _px(prev.close)
            window: list[float] = []
            for (name, when), factors in split_on.items():
                if name == ticker and prev.day < when <= day:
                    window.extend(factors)
            if ratio > JUMP_HI or ratio < JUMP_LO:
                if not _split_explains(ratio, window):
                    print(
                        f"webull sim: {ticker} {day} open not used "
                        f"(unexplained {ratio:.2f}x jump)",
                        flush=True,
                    )
                    continue
        out[(ticker, day)] = {
            "open": opened,
            "high": _px(bar.high),
            "low": _px(bar.low),
            "close": _px(bar.close),
        }
    return out


def fetch_live_bars(tickers: list[str], day: str) -> dict[tuple[str, str], dict]:
    """Official session open after 09:35 ET.

    The committed price store lags the session, so a missing day is read from
    Yahoo with auto_adjust false: split-adjusted, dividends not applied.
    IRONCLAD 26 drops an open that jumps more than 3x from the previous close
    unless a Yahoo split explains it. A name Yahoo did not print is absent.
    """
    if not tickers or not live_fetch_allowed(datetime.now(ET), day):
        return {}
    try:
        import yfinance as yf
        from .price_store import _flatten_actions, _flatten_yf
    except Exception as exc:
        print(f"webull sim: live open unavailable ({exc})", flush=True)
        return {}
    start = (date.fromisoformat(day) - timedelta(days=10)).isoformat()
    end = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    try:
        raw = yf.download(
            tickers, start=start, end=end, group_by="ticker",
            auto_adjust=False, actions=True, progress=False, threads=False,
        )
        flat = _flatten_yf(raw, tickers)
        actions = _flatten_actions(raw, tickers)
    except Exception as exc:
        print(f"webull sim: live open unavailable ({exc})", flush=True)
        return {}
    return official_opens(flat, actions, day)


def _has_open(bars: dict[tuple[str, str], dict], ticker: str, day: str) -> bool:
    bar = bars.get((ticker, day))
    return bool(bar) and bar.get("open") is not None


def adjust_splits(bars: dict[tuple[str, str], dict]) -> dict[tuple[str, str], dict]:
    """Raw session prints. A split while a lot is open is applied by the caller.

    The price store keeps unadjusted Yahoo prints. Two reverse splits are on
    file (DCX, DHY). Fills use the print for that morning, which is the price
    a market order trades. Historical days with no later split match a
    split-adjusted series.
    """
    return bars


def collect_books(today: str | None = None) -> dict[str, list[dict]]:
    books: dict[str, list[dict]] = {}
    books.update(load_ticket_plans())
    books.update(load_excel_plans(today))
    books[book_name("excel_all")] = [empty_plan(
        FIRST_LOCKED,
        "no pre-09:30 plan",
        "Sleeve books use the dated final file. The morning ticket is not the sealed excel plan.",
    )]
    books[book_name("h1")] = load_h1_plans()
    books.update(load_shadow_plans())
    books.update(load_theme_plans())
    days = sorted({plan["date"] for plans in books.values() for plan in plans})
    if FIRST_LOCKED not in days:
        days.append(FIRST_LOCKED)
    # Every collected strategy gets a 10-06 row when that session has no plan yet.
    for name, plans in list(books.items()):
        have = {plan["date"] for plan in plans}
        if FIRST_LOCKED not in have:
            gap = empty_plan(FIRST_LOCKED, "no pre-09:30 plan")
            modes = {plan.get("sizing") for plan in plans if plan.get("sizing")}
            if len(modes) == 1:
                gap["sizing"] = modes.pop()
            plans.append(gap)
        plans.sort(key=lambda plan: plan["date"])
    for name, plans in static_gap_plans(days).items():
        books.setdefault(name, plans)
    return books


def session_calendar(books: dict[str, list[dict]]) -> list[str]:
    days = {plan["date"] for plans in books.values() for plan in plans}
    return sorted(days)


def needed_universe(books: dict[str, list[dict]]) -> tuple[set[str], set[str]]:
    tickers: set[str] = set()
    days: set[str] = set()
    for plans in books.values():
        for plan in plans:
            days.add(plan["date"])
            for pick in plan["picks"]:
                tickers.add(pick.ticker)
            for ticker in plan["exits"]:
                tickers.add(ticker)
    return tickers, days


def _final_rows(path: Path | None) -> dict[str, dict[str, dict]]:
    """Final rows already on disk, keyed by book then date. Loaded verbatim."""
    kept: dict[str, dict[str, dict]] = {}
    for row in read_books(path):
        if row.get("final"):
            kept.setdefault(row["name"], {})[row["date"]] = row
    return kept


def simulate(books: dict[str, list[dict]], now: datetime,
             schedule: Schedule | None = None,
             bars: dict[tuple[str, str], dict] | None = None,
             *,
             books_path: Path | None = None,
             close_marks: dict[tuple[str, str], str] | None = None) -> list[dict]:
    schedule = schedule or load_schedule()
    sessions = session_calendar(books)
    # None means a test sim with no book file. build() passes the real path.
    finals = _final_rows(books_path) if books_path is not None else {}
    if bars is None:
        tickers, days = needed_universe(books)
        bars = adjust_splits(load_bars(tickers, days))
        if live_fetch_allowed(datetime.now(ET), FIRST_LOCKED):
            missing = sorted(t for t in tickers if not _has_open(bars, t, FIRST_LOCKED))
            bars.update(fetch_live_bars(missing, FIRST_LOCKED))

    def bars_for(day: str, tickers: list[str]) -> dict[str, dict]:
        return {
            ticker: bars[(ticker, day)]
            for ticker in tickers
            if _has_open(bars, ticker, day)
        }

    fetched: dict[tuple[str, str], dict] = {}

    def theme_bars_for(day: str, tickers: list[str]) -> dict[str, dict]:
        """Same Yahoo source, fetched for this book's hold days only.

        Results stay out of the shared map so another book does not start
        seeing a print it did not ask for.
        """
        found = bars_for(day, tickers)
        for ticker in tickers:
            if ticker not in found and _has_open(fetched, ticker, day):
                found[ticker] = fetched[(ticker, day)]
        missing = [ticker for ticker in tickers if ticker not in found]
        if not missing or not live_fetch_allowed(datetime.now(ET), day):
            return found
        for (ticker, bar_day), bar in fetch_live_bars(missing, day).items():
            if bar_day == day:
                fetched[(ticker, day)] = bar
                found[ticker] = bar
        return found

    def sandbox_for(day: str) -> str:
        path = PAPER_OPEN / f"{day}_status.json"
        if not path.is_file():
            return "not observed"
        return sandbox_label(json.loads(path.read_text(encoding="utf-8")))

    today = now.astimezone(ET).date().isoformat()
    rows = []
    for name in sorted(books):
        plans = books[name]
        use_sessions = sessions
        use_bars = bars_for
        if is_theme_book(name):
            # Cover days are on this book's calendar only.
            plans = hold_continuations(plans, today)
            use_sessions = sorted(set(sessions) | {plan["date"] for plan in plans})
            use_bars = theme_bars_for
        rows.extend(run_book(
            name, plans, use_sessions, use_bars, schedule, now,
            sandbox_for=sandbox_for,
            sealed_rows=finals.get(name),
            close_marks=close_marks,
        ))
    return rows


def read_books(path: Path | None = None) -> list[dict]:
    dest = path or BOOKS_PATH
    if not dest.is_file():
        return []
    return [json.loads(line) for line in dest.read_text(encoding="utf-8").splitlines() if line.strip()]


def seal_row(row: dict, *, manifest: Path | None = None) -> None:
    if row["date"] <= WATERMARK:
        return
    if not row.get("final"):
        return
    digest = past_day_lock.sha256_text(canon(row) + "\n")
    rows = past_day_lock.load_manifest(manifest)
    past_day_lock._seal(
        rows, RECORD, row["date"], digest, manifest,
        name=row["name"], watermark=WATERMARK,
    )


def write_books(rows: list[dict], *, seal: bool, path: Path | None = None,
                manifest: Path | None = None) -> list[dict]:
    """Keep a built-after line once. Replace an unsealed locked line. Seal finals."""
    dest = path or BOOKS_PATH
    dest.parent.mkdir(parents=True, exist_ok=True)
    existing = {}
    if dest.is_file():
        for line in dest.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            old = json.loads(line)
            existing[(old["name"], old["date"])] = line
    sealed = set()
    if seal:
        for manifest_row in past_day_lock.load_manifest(manifest):
            if manifest_row.get("kind") == "day" and manifest_row.get("record") == RECORD:
                sealed.add((str(manifest_row.get("name")), str(manifest_row.get("date"))))
    kept = dict(existing)
    for row in rows:
        key = (row["name"], row["date"])
        line = canon(row)
        prev = existing.get(key)
        if row["date"] < FIRST_LOCKED and prev:
            old = json.loads(prev)
            if old.get("final"):
                continue
        if key in sealed:
            if prev != line:
                raise past_day_lock.PastDayLockError(
                    f"past-day lock: webull_sim {row['name']} {row['date']} changed. "
                    "Not rescoring a sealed day."
                )
            continue
        kept[key] = line
    ordered = sorted(kept.items(), key=lambda item: (item[0][1], item[0][0]))
    dest.write_text("".join(line + "\n" for _key, line in ordered), encoding="utf-8")
    written = [json.loads(line) for _key, line in ordered]
    if seal:
        for row in written:
            if row.get("final") and row["date"] >= FIRST_LOCKED:
                seal_row(row, manifest=manifest)
    return written


def _money(text: str) -> str:
    if text == "":
        return "—"
    return f"${Decimal(text):,.2f}"


def load_excel_freeze(path: Path | None = None) -> dict:
    """The excel_bot lock. A missing file does not invent fingerprints."""
    dest = path or EXCEL_FREEZE
    if not dest.is_file():
        return {"lock_from": EXCEL_LOCK_FROM, "entries": []}
    try:
        data = json.loads(dest.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"lock_from": EXCEL_LOCK_FROM, "entries": []}
    if not isinstance(data, dict):
        return {"lock_from": EXCEL_LOCK_FROM, "entries": []}
    return data


def excel_signal_day(source: str) -> str | None:
    """Date in ``excel_bot/daily/<date>_excel_bot.md``. That date is the signal date."""
    name = str(source or "").replace("\\", "/").rsplit("/", 1)[-1]
    if "draft" in name or not name.endswith("_excel_bot.md") or len(name) < 11:
        return None
    if name[10] != "_":
        return None
    day = name[:10]
    try:
        date.fromisoformat(day)
    except ValueError:
        return None
    return day


def _manifest_lists_file(source: str, file_day: str, manifest: dict) -> bool:
    entries = manifest.get("entries")
    if not isinstance(entries, list):
        return False
    for entry in entries:
        if isinstance(entry, dict):
            if str(entry.get("signal_date") or "") == file_day:
                return True
            if source and source in json.dumps(entry, ensure_ascii=False):
                return True
        elif isinstance(entry, str) and source and source in entry:
            return True
    return False


def excel_seal_label(source: str, manifest: dict | None = None) -> str:
    """Page label for an excel_bot daily file. Empty when the file is not labelled."""
    file_day = excel_signal_day(source)
    if not file_day:
        return ""
    doc = manifest if manifest is not None else load_excel_freeze()
    lock_from = str(doc.get("lock_from") or EXCEL_LOCK_FROM)
    if file_day < lock_from:
        return EXCEL_PRE_LOCK
    if _manifest_lists_file(source, file_day, doc):
        return EXCEL_MANIFEST_SEAL
    return ""


def display_reason(row: dict) -> str:
    """Reason cell. Unfilled names stay in the note and are shown on the row."""
    reason = row.get("reason") or "traded"
    note = str(row.get("note") or "")
    for marker in ("no open, not filled:", "open not observed:"):
        if marker not in note:
            continue
        names = note.split(marker, 1)[1].strip()
        cut = names.find(" mark not observed")
        if cut >= 0:
            names = names[:cut].strip()
        extra = f"{marker} {names}".strip()
        if extra in reason:
            return reason
        return f"{reason}; {extra}"
    return reason


def display_source(row: dict, manifest: dict | None = None) -> str:
    source = row.get("source") or "—"
    label = excel_seal_label(str(row.get("source") or ""), manifest)
    if not label:
        return source
    return f"{source} — {label}"


def _orders_were_sent(payload: dict) -> bool:
    sent = payload.get("sent")
    if not isinstance(sent, list):
        return False
    for order in sent:
        if not isinstance(order, dict):
            continue
        if str(order.get("order_id") or "").strip():
            return True
        if order.get("ok") is True:
            return True
    return False


def _send_missed_open(payload: dict) -> bool:
    sent = payload.get("sent")
    if not isinstance(sent, list) or not sent:
        return False
    for order in sent:
        if not isinstance(order, dict):
            return False
        if order.get("status") != "missed_deadline":
            return False
        if str(order.get("order_id") or "").strip() or order.get("ok") is True:
            return False
    return True


def unsent_orders_label(payload: dict | None) -> str:
    """Plain reason when the paper-open status file shows no orders went out."""
    if not isinstance(payload, dict) or _orders_were_sent(payload):
        return ""
    status = str(payload.get("status") or "")
    late = _send_missed_open(payload)
    if status not in {
        "missed_deadline", "blocked", "broker_unavailable",
        "not_ready_at_open", "failed", "dry_run", "no_trade",
    } and not late:
        return ""
    day = str(payload.get("date") or "").strip()
    if not day:
        return ""
    if status == "missed_deadline" or late:
        detail = "send started after the 09:30 open (missed deadline)"
    elif status == "blocked":
        error = str(payload.get("error") or "").strip()
        detail = f"{error} (blocked)" if error else "blocked"
    else:
        error = str(payload.get("error") or "").strip()
        words = status.replace("_", " ")
        detail = f"{error} ({words})" if error else words
    return f"No Webull orders sent on {day}: {detail}"


def display_sandbox(row: dict, paper_open: Path | None = None) -> str:
    stored = row.get("sandbox") or "—"
    if row.get("name") != "h1_webull_sim":
        return stored
    day = str(row.get("date") or "")
    path = (paper_open or PAPER_OPEN) / f"{day}_status.json"
    if not path.is_file():
        return stored
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return stored
    label = unsent_orders_label(payload)
    return label or stored


def display_equity(row: dict, notes: dict[tuple[str, str], str] | None = None,
                   close_marks: dict[tuple[str, str], str] | None = None, *,
                   close_notes: dict[tuple[str, str], str] | None = None,
                   escape: bool = False) -> str:
    """Sealed equity, plus a side note that is not stored on the row.

    A passed ``close_marks`` value wins over the add-only close note, so a
    test can show a synthetic close. The page load uses the note when no
    live mark was passed.
    """
    text = _money(row["equity"])
    key = (row.get("name") or "", row.get("date") or "")
    note = (notes or {}).get(key, "")
    mark = (close_marks or {}).get(key)
    if mark in (None, ""):
        mark = (close_notes or {}).get(key)
    if not note and mark in (None, ""):
        return text
    parts = []
    if note:
        parts.append(note)
    if mark not in (None, ""):
        parts.append(f"close equity {_money(mark)}")
        start = row.get("start_cash") or q(START_CASH)
        parts.append(f"day {day_pct_text(start, mark)} using the close")
    body = "; ".join(parts)
    if escape:
        body = html.escape(body)
    return f"{text} sealed ({body})"


def _md_book_row(row: dict, *, manifest: dict | None = None,
                 paper_open: Path | None = None,
                 notes: dict[tuple[str, str], str] | None = None,
                 close_marks: dict[tuple[str, str], str] | None = None,
                 close_notes: dict[tuple[str, str], str] | None = None) -> str:
    return (
        f"| {row['name']} | {row['date']} | {row['section']} | {display_reason(row)} | "
        f"{row.get('sizing') or '—'} | "
        f"{row['picks']} | {display_equity(row, notes, close_marks, close_notes=close_notes)} | {_money(row['fees'])} | "
        f"{row['commit_et'] or '—'} | {display_source(row, manifest)} | "
        f"{(row['commit'] or '—')[:12]} | {display_sandbox(row, paper_open)} |"
    )


def _html_book_row(row: dict, *, manifest: dict | None = None,
                   paper_open: Path | None = None,
                   notes: dict[tuple[str, str], str] | None = None,
                   close_marks: dict[tuple[str, str], str] | None = None,
                   close_notes: dict[tuple[str, str], str] | None = None) -> str:
    reason = display_reason(row)
    sha = (row["commit"] or "—")[:12]
    seal = excel_seal_label(str(row.get("source") or ""), manifest)
    if seal:
        sha = f"{sha} {html.escape(seal)}"
    sandbox = display_sandbox(row, paper_open)
    if sandbox != (row.get("sandbox") or "—"):
        sandbox = html.escape(sandbox)
    return (
        "<tr>"
        f"<td>{row['name']}</td><td>{row['date']}</td><td>{row['section']}</td>"
        f"<td>{reason}</td><td>{row.get('sizing') or '—'}</td>"
        f"<td>{row['picks']}</td><td>{display_equity(row, notes, close_marks, close_notes=close_notes, escape=True)}</td>"
        f"<td>{row['commit_et'] or '—'}</td>"
        f"<td>{sha}</td>"
        f"<td>{sandbox}</td>"
        "</tr>"
    )


def _theme_names(rows: list[dict]) -> list[str]:
    return sorted({row["name"] for row in rows if is_theme_book(row.get("name") or "")})


def render_md(rows: list[dict], schedule: Schedule, *,
              manifest: dict | None = None,
              paper_open: Path | None = None,
              close_marks: dict[tuple[str, str], str] | None = None) -> str:
    if manifest is None:
        manifest = load_excel_freeze()
    notes = load_mark_notes()
    close_notes = load_close_equities()
    locked = [r for r in rows if r["section"] == "locked"]
    built = [r for r in rows if r["section"] == "built_after"]
    visible = [r for r in rows if r["section"] == "not_a_locked_trade"]
    lines = [
        "# Webull simulated books",
        "",
        "Each book is `<strategy>_webull_sim`. These rows do not change any live record.",
        "",
        f"Fees from [{schedule.source_url}]({schedule.source_url}), retrieved {schedule.retrieved}. "
        "Commission $0. SEC fee 0.0000206 per sale dollar. CAT fee 0.000003 per share on buys and sells. "
        "FINRA TAF $0 per share, the current rate printed on that page. "
        "Stock borrow is not published there, so borrow is not modeled. "
        "Algo-order and OTC charges are on the page and are not used.",
        "",
        f"The first fingerprinted day is {FIRST_LOCKED}. Earlier days are "
        "BUILT AFTER THE FACT and are not part of the locked result. "
        "A missed locked day stays missing. Same-day reruns rewrite a row only while it is still unsealed.",
        "",
        "Whole shares at the session open printed in the local Yahoo price store. "
        "The store keeps raw prints. A name with no later split matches a split-adjusted open. "
        "DCX and DHY have reverse splits on file; the fill is the morning print, not a back-adjusted price.",
        "",
        "The stop fills before the target when the same daily bar touches both. "
        "Cash, positions, and fees carry inside a section. The locked section starts again at $10,000.",
        "",
        MISSING_OPEN_RULE,
        "",
        "Excel sleeves buy the next 09:30 open. Their research cards buy the signal-day close. "
        "The two results are not comparable. Theme Radar shorts stay off the real Webull paper account.",
        "",
        SIZING_RULE,
        "",
        f"Locked trade rows: {len(locked)}. Built after the fact: {len(built)}. "
        f"Visible, not a locked trade: {len(visible)}.",
        "",
        "## Locked and not-yet-locked",
        "",
        "| Book | Date | Section | Reason | Sizing | Picks | Equity | Fees | Commit ET | Source | SHA | Sandbox |",
        "|---|---|---|---|---|---:|---:|---:|---|---|---|---|",
    ]
    show = [r for r in rows if r["date"] >= FIRST_LOCKED and not is_theme_book(r["name"])]
    for row in show:
        lines.append(_md_book_row(
            row, manifest=manifest, paper_open=paper_open,
            notes=notes, close_marks=close_marks, close_notes=close_notes,
        ))
    theme_names = _theme_names(rows)
    if theme_names:
        lines += ["", "## Theme Radar short books", ""]
        header = (
            "| Book | Date | Section | Reason | Sizing | Picks | Equity | Fees | Commit ET | Source | SHA | Sandbox |",
            "|---|---|---|---|---|---:|---:|---:|---|---|---|---|",
        )
        for name in theme_names:
            lines.append(f"### {name}")
            lines.append("")
            group = [r for r in rows if r["name"] == name and r["date"] >= FIRST_LOCKED]
            if group:
                lines.extend(header)
                for row in group:
                    lines.append(_md_book_row(
                        row, manifest=manifest, paper_open=paper_open,
                        notes=notes, close_marks=close_marks, close_notes=close_notes,
                    ))
                lines.append("")
            lines.append(THEME_BORROW_NOTE)
            lines.append("")
            lines.append(THEME_LOG_NOTE)
            lines.append("")
    lines += [
        "",
        "## Built after the fact",
        "",
        "These days were assembled from plans that already existed. They are not locked performance.",
        "",
        "| Book | Days | Last equity | Last reason | Sizing |",
        "|---|---:|---:|---|---|",
    ]
    by_name: dict[str, list[dict]] = {}
    for row in built:
        by_name.setdefault(row["name"], []).append(row)
    for name in sorted(by_name):
        group = by_name[name]
        last = group[-1]
        lines.append(
            f"| {name} | {len(group)} | {_money(last['equity'])} | {last['reason'] or 'traded'} | "
            f"{last.get('sizing') or '—'} |"
        )
    lines += [
        "",
        "Per-day provenance (path, commit, commit time in ET) is on every row in `data/webull_sim/days.jsonl`.",
        "",
        "Dispatch after 09:40 ET: `gh workflow run webull_sim.yml --ref main`.",
        "",
    ]
    return "\n".join(lines)


def render_html(rows: list[dict], schedule: Schedule, *,
                manifest: dict | None = None,
                paper_open: Path | None = None,
                close_marks: dict[tuple[str, str], str] | None = None) -> str:
    if manifest is None:
        manifest = load_excel_freeze()
    notes = load_mark_notes()
    close_notes = load_close_equities()
    show = [r for r in rows if r["date"] >= FIRST_LOCKED and not is_theme_book(r["name"])]
    body = [_html_book_row(
        row, manifest=manifest, paper_open=paper_open,
        notes=notes, close_marks=close_marks, close_notes=close_notes,
    ) for row in show]
    table = "\n".join(body)
    theme_blocks = []
    for name in _theme_names(rows):
        group = [r for r in rows if r["name"] == name and r["date"] >= FIRST_LOCKED]
        group_table = "\n".join(
            _html_book_row(
                row, manifest=manifest, paper_open=paper_open,
                notes=notes, close_marks=close_marks, close_notes=close_notes,
            ) for row in group
        )
        theme_blocks.append(
            f"<h2>{name}</h2>\n"
            "<table>\n"
            "<thead><tr><th>Book</th><th>Date</th><th>Section</th><th>Reason</th>"
            "<th>Sizing</th><th>Picks</th><th>Equity</th><th>Commit ET</th>"
            "<th>SHA</th><th>Sandbox</th></tr></thead>\n"
            f"<tbody>\n{group_table}\n</tbody></table>\n"
            f"<p class=\"note\">{THEME_BORROW_NOTE}</p>\n"
            f"<p class=\"note\">{THEME_LOG_NOTE}</p>"
        )
    theme_html = "\n".join(theme_blocks)
    theme_heading = "<h2>Theme Radar short books</h2>\n" if theme_blocks else ""
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Webull sim</title>
<style>
body {{ font: 15px/1.4 system-ui, sans-serif; margin: 24px; color: #1c1c1c; }}
table {{ border-collapse: collapse; width: 100%; }}
th, td {{ border-bottom: 1px solid #ddd; text-align: left; padding: 4px 8px; vertical-align: top; }}
.note {{ max-width: 46rem; }}
</style></head><body>
<h1>Webull simulated books</h1>
<p class="note">Separate from every live book. Fees retrieved {schedule.retrieved} from
<a href="{schedule.source_url}">{schedule.source_url}</a>.
Commission $0, SEC 0.0000206 on sale dollars, CAT 0.000003 per share, FINRA TAF $0.
Borrow is not published, so it is not modeled. The first locked day is {FIRST_LOCKED}.
Earlier days stay in the built-after section.</p>
<p class="note">Excel sim buys the next 09:30 open. The research card buys the signal-day close.
Those numbers are not the same test. Theme Radar shorts are not sent to the Webull paper account.
h1 sandbox fills read the paper-open journal and show "not observed" until a fill price is there.</p>
<p class="note">{SIZING_RULE}</p>
<p class="note">{MISSING_OPEN_RULE}</p>
<table>
<thead><tr><th>Book</th><th>Date</th><th>Section</th><th>Reason</th><th>Sizing</th><th>Picks</th><th>Equity</th><th>Commit ET</th><th>SHA</th><th>Sandbox</th></tr></thead>
<tbody>
{table}
</tbody></table>
{theme_heading}{theme_html}
<p>Machine rows: data/webull_sim/days.jsonl. Write-up: 03_scoreboard/WEBULL_SIM.md.</p>
</body></html>
"""


def publish(rows: list[dict], schedule: Schedule | None = None,
            close_marks: dict[tuple[str, str], str] | None = None) -> None:
    schedule = schedule or load_schedule()
    MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    MD_PATH.write_text(render_md(rows, schedule, close_marks=close_marks), encoding="utf-8")
    HTML_PATH.write_text(render_html(rows, schedule, close_marks=close_marks), encoding="utf-8")


def build(now: datetime, *, seal: bool = False) -> list[dict]:
    if in_write_freeze(now):
        raise SystemExit("webull sim: 03:00–09:40 ET is closed for writes")
    schedule = load_schedule()
    books = collect_books(now.astimezone(ET).date().isoformat())
    print(f"webull sim: {len(books)} books", flush=True)
    close_marks: dict[tuple[str, str], str] = {}
    rows = simulate(books, now, schedule, books_path=BOOKS_PATH, close_marks=close_marks)
    written = write_books(rows, seal=seal)
    publish(written, schedule, close_marks=close_marks)
    return written


def main() -> None:
    now = datetime.now(ET)
    if in_write_freeze(now) and os.environ.get("WEBULL_SIM_ALLOW") != "1":
        print("webull sim: inside 03:00–09:40 ET, not writing")
        return
    seal = os.environ.get("WEBULL_SIM_SEAL", "1") == "1"
    rows = build(now, seal=seal)
    locked = sum(1 for row in rows if row["section"] == "locked")
    print(f"webull sim: {len(rows)} rows, {locked} locked trades")


if __name__ == "__main__":
    main()
