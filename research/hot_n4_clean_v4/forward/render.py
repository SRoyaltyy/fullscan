"""Render the Pages payload from the append-only log. The page does not invent a day.

``today_plan_html`` is the display for the sealed plan. It reads records and
does not append, rewrite, or seal anything. The dashboard pages draw the same
block in the browser from ``log.json``.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from research.hot_n4_clean_v4.forward.book import BOOKS, HOLDUP, current_book
from research.hot_n4_clean_v4.forward.ledger import HERE, load
from src.skip_if_good import is_nyse_holiday

ROOT = HERE.parents[2]
PAGE = HOLDUP.page
LOG_JSON = PAGE / "log.json"
STATUS_JSON = PAGE / "status.json"
NOTES_PATH = HERE / "day_notes.json"
ET = ZoneInfo("America/New_York")
HKT = ZoneInfo("Asia/Hong_Kong")

# Git commits that published these sealed plans. Display only. A plan whose
# sha256 is not here shows the record hash. This map is not a ledger.
SEAL_COMMITS = {
    "b129d5505394941231f1e2c61c37949b9bad920fff9c9b09df3b51e6dd7b6951": "5e6a3d04",
    "68dd183b35ce7783269dd9b4ae699e2518c64f7eb9c953a69acece0675f884b6": "f9c8628a",
}


def load_day_notes(path: Path | None = None) -> dict[str, str]:
    """Date-keyed display notes. An empty file means no notes."""
    src = NOTES_PATH if path is None else path
    if not src.is_file():
        return {}
    data = json.loads(src.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError("day notes")
    notes: dict[str, str] = {}
    for key, value in data.items():
        if isinstance(value, str) and value:
            notes[str(key)] = value
    return notes


def publish_notes() -> None:
    """Copy the notes file onto the pages the site serves. Does not touch logs."""
    if not NOTES_PATH.is_file():
        return
    raw = NOTES_PATH.read_text(encoding="utf-8")
    targets = [ROOT / "dashboard" / "day_notes.json"]
    for book in BOOKS:
        targets.append(book.page / "notes.json")
    for dest in targets:
        dest.parent.mkdir(parents=True, exist_ok=True)
        if dest.is_file() and dest.read_text(encoding="utf-8") == raw:
            continue
        dest.write_text(raw, encoding="utf-8")


def reconcile_status(records: list[dict], status: dict) -> dict:
    """Keep a cached status phase aligned with the log lines for its date.

    The page does not read this file for a day's status. A later plan run
    must not leave ``phase`` at ``plan`` after an open fill is in the log.
    """
    out = dict(status)
    day = out.get("date")
    if not day:
        return out
    view = classify_day(records, str(day))
    if view["mode"] == "filled":
        kind = (view.get("book") or {}).get("kind")
        out["phase"] = kind if kind in ("fill", "mark") else "fill"
    elif view["mode"] == "open":
        out["phase"] = "open_fill"
        out["pending"] = str(day)
    elif view["mode"] == "plan":
        out["phase"] = "plan"
    elif view["mode"] == "missing":
        out["phase"] = "missing"
        out["pending"] = None
    return out


def write_page(records: list[dict] | None = None, status: dict | None = None) -> None:
    rows = load() if records is None else records
    page = current_book().page
    page.mkdir(parents=True, exist_ok=True)
    (page / "log.json").write_text(
        json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8",
    )
    if status is not None:
        (page / "status.json").write_text(
            json.dumps(reconcile_status(rows, status), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    publish_notes()


def session_on_or_after(day: date) -> str:
    """First NYSE session on ``day`` or after it."""
    cursor = day
    for _ in range(14):
        if cursor.weekday() < 5 and not is_nyse_holiday(cursor):
            return cursor.isoformat()
        cursor += timedelta(days=1)
    raise RuntimeError(f"no session on or after {day.isoformat()}")


def focus_session(now: datetime) -> str:
    """The session date the board is for. Weekends and holidays roll forward."""
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return session_on_or_after(now.astimezone(ET).date())


def plan_for(records: list[dict], day: str) -> dict | None:
    """Last sealed plan for ``day``. Missing ``morning_status`` is left missing."""
    found = None
    for row in records:
        if row.get("kind") == "plan" and row.get("date") == day:
            found = row
    return found


def execution_for(records: list[dict], day: str) -> dict:
    """Fill prices already written for ``day``. An unfilled name is absent."""
    buys: dict[str, dict] = {}
    sells: dict[str, dict] = {}

    def take(dest: dict[str, dict], rows: list | None) -> None:
        for row in rows or []:
            ticker = row.get("ticker")
            if ticker and row.get("fill") is not None:
                dest[ticker] = row

    for row in records:
        if row.get("date") != day:
            continue
        kind = row.get("kind")
        if kind in ("open_fill", "fill"):
            take(buys, row.get("buys"))
            take(sells, row.get("sells"))
        elif kind == "mark":
            take(buys, row.get("added_buys"))
            take(sells, row.get("added_sells"))
    return {"buys": buys, "sells": sells}


def seal_clocks(stamp: str) -> tuple[str, str]:
    """``committed_at`` as ET and HKT clocks. Empty when the stamp is missing."""
    if not stamp:
        return "", ""
    when = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return (
        when.astimezone(ET).strftime("%H:%M ET"),
        when.astimezone(HKT).strftime("%H:%M HKT"),
    )


def _num(score) -> str:
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        return str(score)
    return f"{float(score):.8f}".rstrip("0").rstrip(".")


def morning_line(plan: dict) -> str:
    """Score and status. ABSENT only when the record says so.

    A plan sealed before that field existed has no ``morning_status``. That
    is not the same as a missing morning file.
    """
    score = "null" if plan.get("morning_s") is None else _num(plan.get("morning_s"))
    status = plan.get("morning_status")
    if status == "ABSENT":
        why = plan.get("morning_reason") or (plan.get("s_source") or {}).get("reason") or ""
        line = f"morning ABSENT · score {score}"
        if why:
            line += f" · {why}"
        return line
    if not status:
        status = (plan.get("s_source") or {}).get("status")
    if status:
        return f"morning score {score} · status {status}"
    return f"morning score {score}"


def _esc(text) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _px(value) -> str:
    text = f"{float(value):.4f}"
    whole, frac = text.split(".")
    frac = frac.rstrip("0")
    if len(frac) < 2:
        frac = frac.ljust(2, "0")
    return f"{whole}.{frac}"


def _sell_pill(sell: dict, exe: dict) -> str:
    ticker = _esc(sell.get("ticker"))
    shares = sell.get("shares")
    reason = _esc(sell.get("reason") or "")
    hit = exe["sells"].get(sell.get("ticker"))
    if hit is not None:
        return (
            f'<span class="pill sell">{ticker} {shares} {reason} @ {_px(hit["fill"])} filled</span>'
        )
    return f'<span class="pill sell">{ticker} {shares} {reason}</span>'


def _buy_pill(pick: dict, exe: dict) -> str:
    ticker = _esc(pick.get("ticker"))
    sources = "/".join(pick.get("sources") or [])
    src = f" {_esc(sources)}" if sources else ""
    hit = exe["buys"].get(pick.get("ticker"))
    if hit is not None:
        shares = "" if hit.get("shares") is None else f" {hit['shares']}"
        return (
            f'<span class="pill buy">{ticker}{shares}{src} @ {_px(hit["fill"])} filled</span>'
        )
    return f'<span class="pill buy">{ticker}{src} · fills at 09:35 ET open</span>'


def _badge(plan: dict, exe: dict) -> str:
    sells = plan.get("planned_sells") or []
    picks = plan.get("picks") or []
    lines = len(sells) + len(picks)
    priced = sum(1 for row in sells if row.get("ticker") in exe["sells"])
    priced += sum(1 for row in picks if row.get("ticker") in exe["buys"])
    if lines and priced == lines:
        return '<span class="badge filled">filled</span>'
    return '<span class="badge pending">sealed</span>'


def today_plan_html(
    records: list[dict],
    now: datetime | None = None,
    seal_commits: dict[str, str] | None = None,
) -> str:
    """HTML for the session the board is on.

    A sealed plan for that session is shown even before the open fill. Fill
    prices are added beside a line once an open fill, fill, or close mark
    has a price for that ticker. No plan yet uses the empty sentence.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    focus = focus_session(now)
    plan = plan_for(records, focus)
    if plan is None:
        return (
            '<section class="today-plan"><h2>Today\'s plan</h2>'
            f"<p>no plan sealed yet for {_esc(focus)}; plan runs 09:05 ET</p>"
            "</section>"
        )
    exe = execution_for(records, focus)
    commits = SEAL_COMMITS if seal_commits is None else seal_commits
    seal = commits.get(plan.get("sha256") or "") or (plan.get("sha256") or "")[:12]
    et, hkt = seal_clocks(str(plan.get("committed_at") or ""))
    hold = "on" if plan.get("holdup_on") else "off"
    sells = "".join(_sell_pill(row, exe) for row in (plan.get("planned_sells") or []))
    buys = "".join(_buy_pill(row, exe) for row in (plan.get("picks") or []))
    if not sells:
        sells = '<span class="pill flat">no planned sell</span>'
    if not buys:
        buys = '<span class="pill flat">no planned buy</span>'
    clocks = f"<span>{_esc(et)}</span><span>{_esc(hkt)}</span>" if et else ""
    return (
        '<section class="today-plan">'
        "<h2>Today's plan</h2>"
        f"<h3>{_esc(plan.get('date') or focus)} {_badge(plan, exe)}</h3>"
        '<div class="hd">'
        f"<span>seal commit {_esc(seal)}</span>"
        f"{clocks}"
        f"<span>{_esc(morning_line(plan))}</span>"
        f"<span>holdup 2-day hold {hold}</span>"
        "</div>"
        '<div class="lab">Planned sells</div>'
        f'<div class="pills">{sells}</div>'
        '<div class="lab">Planned buys</div>'
        f'<div class="pills">{buys}</div>'
        "</section>"
    )


def render_today_block(path: Path | None = None, now: datetime | None = None) -> str:
    """Render the block from a page's ``log.json``. Does not write."""
    page = path or current_book().page
    records = json.loads((page / "log.json").read_text(encoding="utf-8"))
    return today_plan_html(records, now=now)


def _last(records: list[dict], day: str, kind: str) -> dict | None:
    found = None
    for row in records:
        if row.get("date") == day and row.get("kind") == kind:
            found = row
    return found


def classify_day(records: list[dict], day: str) -> dict:
    """Status of one session from that day's log lines.

    A close fill (``fill`` or ``mark``) wins. An open fill with no close fill
    stays pending. A sealed plan with no open fill stays sealed. ``missing``
    is used only when the log has that record and nothing later for the day.
    ``status.json`` is not read.
    """
    plan = _last(records, day, "plan")
    opened = _last(records, day, "open_fill")
    filled = _last(records, day, "fill")
    marked = _last(records, day, "mark")
    missing = _last(records, day, "missing")
    closes = [row for row in records if row.get("date") == day and row.get("kind") == "close"]
    close = filled or marked
    if close is not None:
        book = close
        if filled is None and marked is not None and opened is not None:
            book = dict(marked)
            book["buys"] = list(opened.get("buys") or []) + list(marked.get("added_buys") or [])
            book["sells"] = list(opened.get("sells") or []) + list(marked.get("added_sells") or [])
        return {"mode": "filled", "status": "", "date": day, "plan": plan, "book": book, "closes": closes}
    if opened is not None:
        return {
            "mode": "open",
            "status": "opened; close fill pending",
            "date": day,
            "plan": plan,
            "book": opened,
            "closes": closes,
        }
    if plan is not None:
        return {"mode": "plan", "status": "plan sealed", "date": day, "plan": plan, "book": None, "closes": closes}
    if missing is not None:
        return {
            "mode": "missing",
            "status": "missing",
            "reason": str(missing.get("reason") or ""),
            "date": day,
            "plan": None,
            "book": None,
            "closes": closes,
        }
    return {"mode": "none", "status": "", "date": day, "plan": None, "book": None, "closes": closes}


def day_status(records: list[dict], day: str) -> str:
    """The status phrase for ``day``. A missing day includes its reason."""
    view = classify_day(records, day)
    if view["mode"] == "missing":
        reason = view.get("reason") or ""
        return f"missing {reason}".strip()
    return str(view.get("status") or "")


def _money(value) -> str:
    number = float(value)
    sign = "-" if number < 0 else ""
    return f"{sign}${abs(number):,.2f}"


def _join_pills(rows, render_one, empty: str) -> str:
    if not rows:
        return f'<span class="pill flat">{_esc(empty)}</span>'
    return "".join(render_one(row) for row in rows)


def _buy_open(row: dict) -> str:
    rank = row.get("rank")
    prefix = "" if rank is None else f"#{rank} "
    return (
        f'<span class="pill buy">{prefix}{_esc(row.get("ticker"))} '
        f'{row.get("shares")} @ {_px(row["fill"])}</span>'
    )


def _sell_open(row: dict) -> str:
    return (
        f'<span class="pill sell">{_esc(row.get("ticker"))} {row.get("shares")} '
        f'@ {_px(row["fill"])} {_esc(row.get("reason") or "")}</span>'
    )


def _held_pill(row: dict, pending: bool) -> str:
    tail = " · P&L pending" if pending else ""
    return (
        f'<span class="pill">{_esc(row.get("ticker"))} {row.get("shares")} '
        f'from {_esc(row.get("entry_date") or "")} hold {row.get("min_hold")}{tail}</span>'
    )


def _plan_buy(row: dict) -> str:
    sources = "+".join(row.get("sources") or [])
    src = f" {_esc(sources)}" if sources else ""
    rank = row.get("rank")
    prefix = "" if rank is None else f"#{rank} "
    return f'<span class="pill buy">{prefix}{_esc(row.get("ticker"))}{src}</span>'


def _plan_sell(row: dict) -> str:
    return (
        f'<span class="pill sell">{_esc(row.get("ticker"))} {row.get("shares")} '
        f'{_esc(row.get("reason") or "")}</span>'
    )


def _close_row(row: dict) -> str:
    entry = "" if row.get("entry_px") is None else _px(row["entry_px"])
    fill = "" if row.get("fill") is None else _px(row["fill"])
    pnl = "" if row.get("pnl_primary") is None else _money(row["pnl_primary"])
    return (
        "<tr>"
        f"<td>{_esc(row.get('date'))}</td>"
        f'<td class="tick">{_esc(row.get("ticker"))}</td>'
        f"<td>{_esc(row.get('shares'))}</td>"
        f"<td>{entry} {_esc(row.get('entry_date') or '')}</td>"
        f"<td>{fill}</td>"
        f"<td>{_esc(row.get('reason') or '')}</td>"
        f"<td>{pnl}</td>"
        "</tr>"
    )


def _note_html(day: str, notes: dict[str, str]) -> str:
    text = notes.get(day) or ""
    if not text:
        return ""
    return f'<p class="day-note">{_esc(text)}</p>'


def day_block_html(
    records: list[dict],
    day: str,
    notes: dict[str, str] | None = None,
) -> str:
    """HTML for one session. Notes are looked up by date, not inlined here."""
    view = classify_day(records, day)
    chosen = load_day_notes() if notes is None else notes
    note = _note_html(day, chosen)
    mode = view["mode"]
    if mode == "open":
        book = view["book"] or {}
        buys = _join_pills(book.get("buys") or [], _buy_open, "no buy")
        sells = _join_pills(book.get("sells") or [], _sell_open, "no sell")
        held = _join_pills(
            book.get("holdings") or [], lambda row: _held_pill(row, True), "flat",
        )
        return (
            f'<article class="day open" data-date="{_esc(day)}">'
            f'<h3>{_esc(day)} <span class="badge pending">opened; close fill pending</span></h3>'
            '<div class="lab">Buys</div>'
            f'<div class="pills">{buys}</div>'
            '<div class="lab">Sells</div>'
            f'<div class="pills">{sells}</div>'
            '<div class="lab">Held positions</div>'
            f'<div class="pills">{held}</div>'
            '<div class="lab">P&amp;L</div>'
            '<div class="pills"><span class="pill flat">pending</span></div>'
            f"{note}"
            "</article>"
        )
    if mode == "plan":
        plan = view["plan"] or {}
        buys = _join_pills(plan.get("picks") or [], _plan_buy, "no pick")
        sells = _join_pills(plan.get("planned_sells") or [], _plan_sell, "no planned sell")
        return (
            f'<article class="day pending" data-date="{_esc(day)}">'
            f'<h3>{_esc(day)} <span class="badge pending">plan sealed</span></h3>'
            '<div class="lab">Planned buys</div>'
            f'<div class="pills">{buys}</div>'
            '<div class="lab">Planned sells</div>'
            f'<div class="pills">{sells}</div>'
            f"{note}"
            "</article>"
        )
    if mode == "missing":
        reason = str(view.get("reason") or "")
        return (
            f'<article class="day pending" data-date="{_esc(day)}">'
            f'<h3>{_esc(day)} <span class="badge pending">missing</span></h3>'
            f'<div class="hd"><span>{_esc(reason)}</span></div>'
            f"{note}"
            "</article>"
        )
    if mode == "filled":
        closes = view.get("closes") or []
        rows = "".join(_close_row(row) for row in closes) or '<tr><td colspan="7">none</td></tr>'
        return (
            f'<article class="day filled" data-date="{_esc(day)}">'
            f'<h3>{_esc(day)} <span class="badge filled">filled after the close</span></h3>'
            '<div class="lab">Closed-trade P&amp;L, 0.5%/side</div>'
            f"<table><tbody>{rows}</tbody></table>"
            f"{note}"
            "</article>"
        )
    return ""
