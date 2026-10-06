"""Webull simulated books. One new book per strategy, named ``<strategy>_webull_sim``.

Each book reads that strategy's sealed pre-09:30 ET plan, buys whole shares
at the session open, and lets the stop fill before the target when a bar
touches both. Cash, positions, and fees carry day to day inside one section.

2026-10-06 is the first day that can be fingerprinted. Earlier days are
built once, labelled BUILT AFTER THE FACT, and are not mixed into the
locked result. A strategy with no sealed plan still gets a row that says
why. Nothing here writes an existing live book.

The scheduled run stays shut from 03:00 until 09:40 ET.
"""
from __future__ import annotations

import csv
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
MD_PATH = ROOT / "03_scoreboard" / "WEBULL_SIM.md"
HTML_PATH = ROOT / "dashboard" / "webull-sim" / "index.html"
H1_LOG = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1" / "h1_log.jsonl"
TICKET_DIR = ROOT / "data" / "day_board"
EXCEL_DIR = ROOT / "excel_bot" / "daily"
EXCEL_STRATS = ROOT / "excel_bot" / "strategies"
SHADOW_DIR = ROOT / "research" / "forward_shadow_v1" / "ledger"
PAPER_OPEN = ROOT / "data" / "paper_open"
PRICE_PATH = ROOT / "data" / "prices" / "ohlc.parquet"
ACTIONS_PATH = ROOT / "data" / "prices" / "actions.parquet"

FIRST_LOCKED = "2026-10-06"
WATERMARK = "2026-10-05"
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


@dataclass
class Pick:
    ticker: str
    side: str
    stop: Decimal | None = None
    target_pct: Decimal | None = None
    hold_sessions: int | None = None


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

    ordered = sorted(picks, key=lambda p: (p.ticker, p.side))
    for i, pick in enumerate(ordered):
        bar = bars.get(pick.ticker)
        opened = _bar_px(bar, "open")
        if opened is None:
            fills.append({
                "ticker": pick.ticker, "side": pick.side, "shares": 0,
                "price": None, "fee": Decimal("0"), "reason": "open not observed",
                "date": date,
            })
            continue
        slots = len(ordered) - i
        budget = account.cash / Decimal(slots)
        entry_side = "buy" if pick.side == "long" else "sell"
        shares = whole_shares(budget, opened, schedule, entry_side)
        if shares < 1:
            fills.append({
                "ticker": pick.ticker, "side": entry_side, "shares": 0,
                "price": opened, "fee": Decimal("0"), "reason": "no whole share",
                "date": date,
            })
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
        if pick.hold_sessions is not None:
            exit_index = session_index + pick.hold_sessions
        account.lots.append(Lot(
            ticker=pick.ticker, side=pick.side, shares=shares, entry_px=opened,
            entry_date=date, stop=pick.stop, target=target, exit_index=exit_index,
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


def in_write_freeze(now: datetime) -> bool:
    local = now.astimezone(ET)
    start = local.replace(hour=3, minute=0, second=0, microsecond=0)
    end = local.replace(hour=9, minute=40, second=0, microsecond=0)
    return start <= local < end


def session_open(day: str) -> datetime:
    return datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=ET)


def before_open(when: datetime, day: str) -> bool:
    return when.astimezone(ET) < session_open(day)


def clock_et(when: datetime) -> str:
    return when.astimezone(ET).strftime("%H:%M")


def next_weekday(day: str) -> str:
    cursor = date.fromisoformat(day) + timedelta(days=1)
    while cursor.weekday() >= 5:
        cursor += timedelta(days=1)
    return cursor.isoformat()


def book_name(strategy: str) -> str:
    stem = strategy if strategy.endswith("_webull_sim") else f"{strategy}_webull_sim"
    return stem


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
    if day < FIRST_LOCKED:
        return (not tradable) or opens_ok
    if now.astimezone(ET) < session_open(day) + timedelta(minutes=10):
        return False
    if not tradable:
        return True
    return opens_ok


def run_book(
    name: str,
    plans: list[dict],
    sessions: list[str],
    bars_for,
    schedule: Schedule,
    now: datetime,
    sandbox_for=None,
) -> list[dict]:
    """Sequential sim. Locked cash starts over at the first locked day."""
    index = {day: i for i, day in enumerate(sessions)}
    built = Account()
    locked = Account()
    rows = []
    for plan in plans:
        day = plan["date"]
        account = built if day < FIRST_LOCKED else locked
        tradable, reason = plan["tradable"], plan["reason"]
        note = plan.get("note") or ""
        # A day before the lock can still be simulated once. The late commit
        # stays visible, and the result stays out of the locked section.
        if day < FIRST_LOCKED and plan["picks"] and not tradable:
            note = (note + " " + reason).strip()
            reason = ""
            tradable = True
        tickers = [pick.ticker for pick in plan["picks"]]
        tickers += [lot.ticker for lot in account.lots]
        tickers += list(plan["exits"])
        bars = bars_for(day, tickers)
        opens_ok = True
        if tradable:
            for pick in plan["picks"]:
                bar = bars.get(pick.ticker)
                if not bar or bar.get("open") is None:
                    opens_ok = False
                    break
            for ticker in plan["exits"]:
                if any(lot.ticker == ticker for lot in account.lots):
                    bar = bars.get(ticker)
                    if not bar or bar.get("open") is None:
                        opens_ok = False
        fills: list[dict] = []
        if tradable and opens_ok:
            fills = apply_session(
                account, plan["picks"], plan["exits"], bars, schedule,
                day, index[day],
            )
        elif tradable and not opens_ok:
            reason = "open not observed"
        equity, missing_marks = mark_equity(account, bars)
        if missing_marks:
            note = (note + " mark not observed, carried at entry: " + ",".join(missing_marks)).strip()
        locked_trade = bool(
            day >= FIRST_LOCKED and tradable and opens_ok and reason == ""
        )
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
            final=final_row(day, now, tradable, opens_ok),
            cash=account.cash,
            fees=account.fees,
            equity=equity,
            fills=fills,
            positions=lot_snapshot(account),
            picks=len(plan["picks"]),
            borrow=plan.get("borrow") or "",
            sandbox=sandbox,
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


def load_excel_plans() -> dict[str, list[dict]]:
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
    return {book_name(k): v for k, v in books.items()}


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
        note = "Sealed h1 plan log. Sim cash starts at $10,000 and is not the research book's cash."
        if row.get("holdup_on"):
            note += " holdup_on."
        plans.append(_plan(day, rel, sha, when, before, picks, exits, note=note))
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


def parse_theme_log(text: str, commit: str, when: datetime | None) -> dict[str, list[dict]]:
    reader = csv.DictReader(io.StringIO(text))
    fields = set(reader.fieldnames or [])
    rate_field = next((name for name in BORROW_FIELDS if name in fields), "")
    books: dict[str, list[dict]] = {}
    grouped: dict[tuple[str, str], list[Pick]] = {}
    for row in reader:
        cell = row.get("cell") or "theme_radar"
        day = row.get("entry") or row.get("date") or ""
        ticker = (row.get("ticker") or "").upper()
        if not day or not ticker:
            continue
        try:
            hold = int(row.get("hold_days") or "0")
        except ValueError:
            hold = 0
        grouped.setdefault((cell, day), []).append(
            Pick(ticker=ticker, side="short", hold_sessions=hold or None)
        )
        if rate_field and row.get(rate_field):
            grouped[(cell, day)]  # borrow attached below
    # Re-read borrow per day from the first row that has it.
    borrow_by: dict[tuple[str, str], str] = {}
    if rate_field:
        for row in csv.DictReader(io.StringIO(text)):
            cell = row.get("cell") or "theme_radar"
            day = row.get("entry") or row.get("date") or ""
            if row.get(rate_field):
                borrow_by[(cell, day)] = f"log field {rate_field}={row[rate_field]}"
    for (cell, day), picks in sorted(grouped.items()):
        borrow = borrow_by.get((cell, day), "borrow not modeled")
        before = bool(when and before_open(when, day))
        note = (
            "Research short. Off the real Webull paper account. "
            "Enters at the 09:30 open and covers at the open after the hold. "
            + borrow + "."
        )
        plan = _plan(
            day, "SRoyaltyy/theme-radar:research/shadow_log/log.csv",
            commit, when, before, picks, [], note=note, borrow=borrow,
        )
        # Days before the lock stay in the built-after section even when the
        # log commit is the morning of the first locked day.
        books.setdefault(book_name(f"theme_radar_{cell}"), []).append(plan)
    return books


def load_theme_plans() -> dict[str, list[dict]]:
    proc = subprocess.run(
        ["gh", "api", "repos/SRoyaltyy/theme-radar/contents/research/shadow_log/log.csv",
         "-H", "Accept: application/vnd.github.raw"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    meta = subprocess.run(
        ["gh", "api", "repos/SRoyaltyy/theme-radar/commits?path=research/shadow_log/log.csv&per_page=1"],
        cwd=ROOT, check=False, capture_output=True, text=True,
    )
    source = "SRoyaltyy/theme-radar:research/shadow_log/log.csv"
    if proc.returncode != 0 or not proc.stdout.strip():
        return {
            book_name(f"theme_radar_{cell}"): [
                empty_plan(FIRST_LOCKED, "no pre-09:30 plan", "theme-radar log not readable this run")
            ]
            for cell in ("fpe_delta_t3_earn_today_3d", "fresh_dcp_t1_ep_ge03_2d", "fresh_dcp_t1_avoid_ah_3d")
        }
    when = None
    sha = ""
    if meta.returncode == 0 and meta.stdout.strip().startswith("["):
        commits = json.loads(meta.stdout)
        if commits:
            sha = commits[0]["sha"]
            when = datetime.fromisoformat(
                commits[0]["commit"]["committer"]["date"].replace("Z", "+00:00")
            )
    books = parse_theme_log(proc.stdout, sha, when)
    for plans in books.values():
        if FIRST_LOCKED in {plan["date"] for plan in plans}:
            continue
        before = bool(when and before_open(when, FIRST_LOCKED))
        note = (
            "Research short. Off the real Webull paper account. "
            "borrow not modeled."
        )
        plans.append(_plan(
            FIRST_LOCKED, source, sha, when, before, [], [],
            note=note, borrow="borrow not modeled",
        ))
    return books


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


def fetch_live_bars(tickers: list[str], day: str) -> dict[tuple[str, str], dict]:
    """Yahoo split-adjusted session bar, only after the open. Empty on failure."""
    if not tickers or not live_fetch_allowed(datetime.now(ET), day):
        return {}
    try:
        import yfinance as yf
        from .price_store import _flatten_yf
    except Exception as exc:
        print(f"webull sim: live open unavailable ({exc})", flush=True)
        return {}
    end = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
    try:
        raw = yf.download(
            tickers, start=day, end=end, auto_adjust=True,
            progress=False, threads=False,
        )
        flat = _flatten_yf(raw, tickers)
    except Exception as exc:
        print(f"webull sim: live open unavailable ({exc})", flush=True)
        return {}
    out = {}
    if flat is None or flat.empty:
        return out
    flat["day"] = flat["date"].dt.strftime("%Y-%m-%d")
    for row in flat.itertuples(index=False):
        if row.day != day:
            continue
        out[(str(row.ticker).upper(), day)] = {
            "open": None if row.open != row.open else float(row.open),
            "high": None if row.high != row.high else float(row.high),
            "low": None if row.low != row.low else float(row.low),
            "close": None if row.close != row.close else float(row.close),
        }
    return out


def adjust_splits(bars: dict[tuple[str, str], dict]) -> dict[tuple[str, str], dict]:
    """Raw session prints. A split while a lot is open is applied by the caller.

    The price store keeps unadjusted Yahoo prints. Two reverse splits are on
    file (DCX, DHY). Fills use the print for that morning, which is the price
    a market order trades. Historical days with no later split match a
    split-adjusted series.
    """
    return bars


def collect_books() -> dict[str, list[dict]]:
    books: dict[str, list[dict]] = {}
    books.update(load_ticket_plans())
    books.update(load_excel_plans())
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
            plans.append(empty_plan(FIRST_LOCKED, "no pre-09:30 plan"))
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


def simulate(books: dict[str, list[dict]], now: datetime,
             schedule: Schedule | None = None,
             bars: dict[tuple[str, str], dict] | None = None) -> list[dict]:
    schedule = schedule or load_schedule()
    sessions = session_calendar(books)
    if bars is None:
        tickers, days = needed_universe(books)
        bars = adjust_splits(load_bars(tickers, days))
        if live_fetch_allowed(datetime.now(ET), FIRST_LOCKED):
            missing = sorted(t for t in tickers if (t, FIRST_LOCKED) not in bars)
            bars.update(fetch_live_bars(missing, FIRST_LOCKED))

    def bars_for(day: str, tickers: list[str]) -> dict[str, dict]:
        return {ticker: bars[(ticker, day)] for ticker in tickers if (ticker, day) in bars}

    def sandbox_for(day: str) -> str:
        path = PAPER_OPEN / f"{day}_status.json"
        if not path.is_file():
            return "not observed"
        return sandbox_label(json.loads(path.read_text(encoding="utf-8")))

    rows = []
    for name in sorted(books):
        rows.extend(run_book(
            name, books[name], sessions, bars_for, schedule, now,
            sandbox_for=sandbox_for,
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


def render_md(rows: list[dict], schedule: Schedule) -> str:
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
        "Excel sleeves buy the next 09:30 open. Their research cards buy the signal-day close. "
        "The two results are not comparable. Theme Radar shorts stay off the real Webull paper account.",
        "",
        f"Locked trade rows: {len(locked)}. Built after the fact: {len(built)}. "
        f"Visible, not a locked trade: {len(visible)}.",
        "",
        "## Locked and not-yet-locked",
        "",
        "| Book | Date | Section | Reason | Picks | Equity | Fees | Commit ET | Source | SHA | Sandbox |",
        "|---|---|---|---|---:|---:|---:|---|---|---|---|",
    ]
    show = [r for r in rows if r["date"] >= FIRST_LOCKED]
    for row in show:
        lines.append(
            f"| {row['name']} | {row['date']} | {row['section']} | {row['reason'] or 'traded'} | "
            f"{row['picks']} | {_money(row['equity'])} | {_money(row['fees'])} | "
            f"{row['commit_et'] or '—'} | {row['source'] or '—'} | "
            f"{(row['commit'] or '—')[:12]} | {row['sandbox'] or '—'} |"
        )
    lines += [
        "",
        "## Built after the fact",
        "",
        "These days were assembled from plans that already existed. They are not locked performance.",
        "",
        "| Book | Days | Last equity | Last reason |",
        "|---|---:|---:|---|",
    ]
    by_name: dict[str, list[dict]] = {}
    for row in built:
        by_name.setdefault(row["name"], []).append(row)
    for name in sorted(by_name):
        group = by_name[name]
        last = group[-1]
        lines.append(
            f"| {name} | {len(group)} | {_money(last['equity'])} | {last['reason'] or 'traded'} |"
        )
    lines += [
        "",
        "Per-day provenance (path, commit, commit time in ET) is on every row in `data/webull_sim/days.jsonl`.",
        "",
        "Dispatch after 09:40 ET: `gh workflow run webull_sim.yml --ref main`.",
        "",
    ]
    return "\n".join(lines)


def render_html(rows: list[dict], schedule: Schedule) -> str:
    show = [r for r in rows if r["date"] >= FIRST_LOCKED]
    body = []
    for row in show:
        reason = row["reason"] or "traded"
        body.append(
            "<tr>"
            f"<td>{row['name']}</td><td>{row['date']}</td><td>{row['section']}</td>"
            f"<td>{reason}</td><td>{row['picks']}</td><td>{_money(row['equity'])}</td>"
            f"<td>{row['commit_et'] or '—'}</td>"
            f"<td>{(row['commit'] or '—')[:12]}</td>"
            f"<td>{row['sandbox'] or '—'}</td>"
            "</tr>"
        )
    table = "\n".join(body)
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
<table>
<thead><tr><th>Book</th><th>Date</th><th>Section</th><th>Reason</th><th>Picks</th><th>Equity</th><th>Commit ET</th><th>SHA</th><th>Sandbox</th></tr></thead>
<tbody>
{table}
</tbody></table>
<p>Machine rows: data/webull_sim/days.jsonl. Write-up: 03_scoreboard/WEBULL_SIM.md.</p>
</body></html>
"""


def publish(rows: list[dict], schedule: Schedule | None = None) -> None:
    schedule = schedule or load_schedule()
    MD_PATH.parent.mkdir(parents=True, exist_ok=True)
    HTML_PATH.parent.mkdir(parents=True, exist_ok=True)
    MD_PATH.write_text(render_md(rows, schedule), encoding="utf-8")
    HTML_PATH.write_text(render_html(rows, schedule), encoding="utf-8")


def build(now: datetime, *, seal: bool = False) -> list[dict]:
    if in_write_freeze(now):
        raise SystemExit("webull sim: 03:00–09:40 ET is closed for writes")
    schedule = load_schedule()
    books = collect_books()
    print(f"webull sim: {len(books)} books", flush=True)
    rows = simulate(books, now, schedule)
    written = write_books(rows, seal=seal)
    publish(written, schedule)
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
