"""Open-fill at the session open, and the later close mark.

The plan stays the only record of the picks. An open-fill marks the planned
buys and sells at the official open and carries no close equity, so P&L for
what is still open stays pending. The post-close fill adds close records and
the close mark, plus any name the open-fill could not price. It does not
append a second fill for a session and symbol already filled.

A missing plan, a second run, or no trustworthy open appends nothing.
"""
from __future__ import annotations

import math

from research.hot_n4_clean_v4.forward.ledger import (
    book_dates,
    canonical_bytes,
    effective_open_fill,
    kind_on,
    latest_open_correction,
    open_plan,
    plan_on,
)
# Cyrus approved this restatement of the 2026-09-28 open fill. The sealed
# line used the 2026-09-25 bar for SRFM and SECZ. A correction records these
# words and the sizing code's result at the true session opens.
STALE_OPEN_REASON = "stale_bar: previous-session bar used for SRFM, SECZ"
STALE_OPEN_APPROVED_BY = "Cyrus 2026-09-28 21:02 ET"
from research.hot_n4_clean_v4.forward.opens import OPEN_SOURCE, _positive, session_open
from research.hot_n4_clean_v4.forward.planfill import book_state_before, fill_book
from research.hot_n4_clean_v4.forward.book import current_book


def _has_bar(bars: dict, session: str) -> bool:
    for blob in bars["stored"].values():
        dates = blob.get("date") or []
        if session in dates:
            return True
    return False


def inject_opens(bars: dict, session: str, opens: dict[str, float]) -> dict:
    """Copy ``bars`` and append an in-memory open for names that have one.

    The copied close equals the open so a not-yet-final close cannot invent
    a close-over-open leg. The copy is not written to ``prices.jsonl``.
    A date already stored is left as stored.
    """
    out: dict = {"feat": {}, "stored": {}}
    for side in ("feat", "stored"):
        for ticker, blob in (bars.get(side) or {}).items():
            copied = {}
            for key, value in blob.items():
                if key == "date":
                    copied[key] = list(value)
                elif key == "adjusted":
                    copied[key] = value
                else:
                    copied[key] = [float(item) for item in value]
            out[side][ticker] = copied
    for ticker, raw in opens.items():
        try:
            op = float(raw)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(op) or op <= 0:
            continue
        name = str(ticker).upper()
        blob = out["stored"].setdefault(
            name, {"adjusted": True, "close": [], "date": [], "open": []},
        )
        dates = blob["date"]
        if session in dates:
            i = dates.index(session)
            if float(blob["open"][i]) != op:
                raise RuntimeError(
                    f"stored open for {name} on {session} no longer matches the session open"
                )
            continue
        if dates and session < dates[-1]:
            continue
        dates.append(session)
        blob["open"].append(op)
        blob["close"].append(op)
    return out


def _same_fill(old: dict, new: dict) -> bool:
    if int(old["shares"]) != int(new["shares"]):
        return False
    return float(old["fill"]) == float(new["fill"])


def open_fill_body(plan: dict, state: dict, bars: dict, fees: dict, index: dict[str, int], opens: dict[str, float]) -> dict:
    """One open-fill body. No equity, so the close P&L is not sealed here."""
    view = inject_opens(bars, plan["date"], opens)
    fill, _closes, _state = fill_book(plan, state, view, fees, index)
    body = {key: value for key, value in fill.items() if key != "equity_primary"}
    body["kind"] = "open_fill"
    body["open_source"] = OPEN_SOURCE
    body["pnl_status"] = "pending"
    body["recipe"] = current_book().recipe
    return body


def close_mark(plan: dict, opened: dict, prior: dict, bars: dict, fees: dict, index: dict[str, int]) -> tuple[dict, list[dict]]:
    """Close records and the close mark for an open-fill already sealed.

    Buys and sells already on ``opened`` are not returned again. Names the
    open-fill left unpriced are ``added_buys`` / ``added_sells`` when the
    close bar now has their open. A sealed fill that disagrees with that
    open raises, and the caller appends nothing.
    """
    full, closes, _state = fill_book(plan, prior, bars, fees, index)
    old_buys = {row["ticker"]: row for row in opened.get("buys") or []}
    old_sells = {row["ticker"]: row for row in opened.get("sells") or []}
    new_buys = {row["ticker"]: row for row in full.get("buys") or []}
    new_sells = {row["ticker"]: row for row in full.get("sells") or []}
    for ticker, row in old_buys.items():
        match = new_buys.get(ticker)
        if match is None or not _same_fill(row, match):
            raise RuntimeError(
                f"stored open for {ticker} on {plan['date']} no longer matches the sealed fill"
            )
    for ticker, row in old_sells.items():
        match = new_sells.get(ticker)
        if match is None or not _same_fill(row, match):
            raise RuntimeError(
                f"stored open for {ticker} on {plan['date']} no longer matches the sealed fill"
            )
    added_buys = [row for row in full["buys"] if row["ticker"] not in old_buys]
    added_sells = [row for row in full["sells"] if row["ticker"] not in old_sells]
    mark = {
        "added_buys": added_buys,
        "added_sells": added_sells,
        "cash_primary": full["cash_primary"],
        "date": plan["date"],
        "equity_primary": full["equity_primary"],
        "holdings": full["holdings"],
        "kind": "mark",
        "open_fill_sha256": opened["sha256"],
        "plan_sha256": plan["sha256"],
        "pnl_status": "marked",
        "recipe": current_book().recipe,
    }
    return mark, closes


def _halt_names(records: list[dict], session: str) -> set[str]:
    """Held names and IWM. Their open has to be in hand before any fill is sealed."""
    return set(book_state_before(records, session)["pos"]) | {"IWM"}


def open_legs(plan: dict, held: set[str]) -> list[str]:
    """Buy and sell tickers this open fill would seal."""
    held_names = {str(ticker).upper() for ticker in held}
    sells = [str(row["ticker"]).upper() for row in plan.get("planned_sells") or []]
    buys = [
        str(row["ticker"]).upper()
        for row in plan.get("picks") or []
        if str(row["ticker"]).upper() not in held_names
    ]
    return sorted(set(sells) | set(buys))


def refuse_stale_open(session: str, legs: list[str], live: dict, stored: dict) -> str | None:
    """Why this open fill must not seal, or None when every leg's bar is ``session``.

    Each buy and sell leg needs a bar dated ``session`` in America/New_York.
    A missing bar or any other date refuses the whole fill. When that session
    bar is absent and the open equals the prior session's open or close, the
    reason says so.
    """
    from research.hot_n4_clean_v4.run_study import bar_on, prev_session

    prior = prev_session(session)
    lines = []
    for ticker in legs:
        if session_open(ticker, session, live, stored) is not None:
            continue
        bar = live.get(ticker) or {}
        when = bar.get("date")
        shown = when or "missing"
        text = f"{ticker} bar {shown} is not {session}"
        op = _positive(bar.get("open"))
        prev = bar_on(stored.get(ticker), prior) if stored else None
        if op is not None and prev and when != session:
            if float(op) == float(prev["open"]):
                text += "; fill equals the prior session open"
            elif float(op) == float(prev["close"]):
                text += "; fill equals the prior session close"
        lines.append(text)
    if not lines:
        return None
    return "open fill not sealed: " + "; ".join(lines)


def decide_open_fill(
    records: list[dict],
    session: str,
    opens: dict[str, float],
    bars: dict,
    fees: dict,
    index: dict[str, int],
) -> tuple[list[dict], str | None]:
    """Bodies to append for the 09:35 run, or nothing.

    ``None`` as the reason means a clean no-op: there is no plan for
    ``session``, or that session is already filled. A reason means the
    open was not trustworthy and the later fill should write the trades.
    """
    plan = plan_on(records, session)
    if plan is None:
        return [], None
    if any(row["kind"] in ("open_fill", "fill", "mark") and row["date"] == session for row in records):
        return [], None
    clean = {}
    for ticker, raw in (opens or {}).items():
        try:
            op = float(raw)
        except (TypeError, ValueError):
            continue
        if math.isfinite(op) and op > 0:
            clean[str(ticker).upper()] = op
    if not clean:
        return [], "no trustworthy session open; appended nothing so the post-close fill can write it"
    missing = sorted(_halt_names(records, session) - set(clean))
    if missing:
        return [], (
            "no trustworthy open for " + ", ".join(missing)
            + "; appended nothing so the post-close fill can write it"
        )
    state = book_state_before(records, session)
    body = open_fill_body(plan, state, bars, fees, index, clean)
    return [body], None


def decide_fill(
    records: list[dict],
    bars: dict,
    fees: dict,
    index: dict[str, int],
) -> tuple[list[dict], str | None]:
    """Bodies for the 21:30 run.

    No open-fill: the whole fill and its closes, as before. An open-fill
    already sealed: closes, the close mark, and only names that open-fill
    did not price. A resolved plan appends nothing.
    """
    pending = open_plan(records)
    if pending is None:
        return [], None
    session = pending["date"]
    if not _has_bar(bars, session):
        latest = ""
        for blob in bars["stored"].values():
            dates = blob.get("date") or []
            if dates and dates[-1] > latest:
                latest = dates[-1]
        return [], (
            f"price store latest bar is {latest or 'empty'}; {session} open is not in the file yet"
        )
    prior = book_state_before(records, session)
    opened = effective_open_fill(records, session)
    if opened is None:
        fill, closes, _state = fill_book(pending, prior, bars, fees, index)
        return [fill, *closes], None
    mark, closes = close_mark(pending, opened, prior, bars, fees, index)
    return [mark, *closes], None


def _leg_copy(row: dict | None) -> dict | None:
    if row is None:
        return None
    copied = dict(row)
    if "sources" in copied:
        copied["sources"] = list(copied["sources"])
    return copied


def _leg_changed(old: dict | None, new: dict | None) -> bool:
    if old is None or new is None:
        return True
    if int(old["shares"]) != int(new["shares"]):
        return True
    if float(old["fill"]) != float(new["fill"]):
        return True
    if float(old.get("fee") or 0) != float(new.get("fee") or 0):
        return True
    return False


def changed_legs(sealed: dict, corrected: dict) -> list[dict]:
    """Sealed versus corrected values for each buy or sell that moved."""
    out = []
    for side, key in (("buy", "buys"), ("sell", "sells")):
        old = {row["ticker"]: row for row in sealed.get(key) or []}
        new = {row["ticker"]: row for row in corrected.get(key) or []}
        for ticker in sorted(set(old) | set(new)):
            before = old.get(ticker)
            after = new.get(ticker)
            if not _leg_changed(before, after):
                continue
            out.append({
                "corrected": _leg_copy(after),
                "sealed": _leg_copy(before),
                "side": side,
                "ticker": ticker,
            })
    return out


def require_nonnegative_cash(cash: float, label: str) -> float:
    """Fail loudly when a correction would leave the book short cash."""
    value = float(cash)
    if value < 0:
        raise RuntimeError(f"{label} cash_primary {value} is negative")
    return value


def correction_opens(plan: dict, held: set[str], bars: dict) -> dict[str, float]:
    """True session opens from the stored bars, for every buy and sell leg."""
    from research.hot_n4_clean_v4.forward.opens import stored_opens

    legs = open_legs(plan, held)
    names = sorted(set(legs) | {str(ticker).upper() for ticker in held} | {"IWM"})
    found = stored_opens(bars["stored"], names, plan["date"])
    missing = [name for name in legs if name not in found]
    if missing:
        raise RuntimeError("no stored session open for " + ", ".join(missing))
    return found


def bars_with_stored_session(bars: dict, session: str, names: list[str]) -> dict:
    """Return ``bars`` with a stored session open for every name.

    The book's own ``prices.jsonl`` wins. A name that is not in that file
    is filled from the other forward book's ``prices.jsonl``. h1 has no
    price file yet; the 2026-09-28 bars are already in the holdup file.
    Nothing is written.
    """
    from research.hot_n4_clean_v4.forward.book import BOOKS
    from research.hot_n4_clean_v4.forward.opens import stored_opens
    from research.hot_n4_clean_v4.forward.prices import load_price_rows, overlay_rows

    found = stored_opens(bars["stored"], names, session)
    missing = [name for name in names if name not in found]
    if not missing:
        return bars
    want = set(missing)
    extra = []
    seen = set()
    for book in BOOKS:
        if book.folder == current_book().folder:
            continue
        path = book.folder / "prices.jsonl"
        if not path.is_file():
            continue
        for row in load_price_rows(book.folder):
            if row.get("date") != session or row.get("ticker") not in want:
                continue
            key = row["ticker"]
            if key in seen:
                continue
            seen.add(key)
            extra.append(row)
    if not extra:
        return bars
    return overlay_rows(bars, extra)


def _fills_match_stored(body: dict, bars: dict) -> None:
    from research.hot_n4_clean_v4.run_study import open_px

    session = body["date"]
    for row in list(body.get("buys") or []) + list(body.get("sells") or []):
        op = open_px(bars["stored"], row["ticker"], session)
        if op is None or float(op) != float(row["fill"]):
            raise RuntimeError(
                f"corrected open for {row['ticker']} on {session} does not match the stored bar"
            )


def correction_body(
    plan: dict,
    sealed_open: dict,
    corrected_open: dict,
    reason: str,
    approved_by: str,
) -> dict:
    """One correction. ``corrected_open`` is the open-fill sizing result."""
    require_nonnegative_cash(corrected_open["cash_primary"], corrected_open.get("recipe") or plan["date"])
    return {
        "approved_by": approved_by,
        "changed_legs": changed_legs(sealed_open, corrected_open),
        "corrected": corrected_open,
        "date": plan["date"],
        "kind": "open_fill_correction",
        "open_fill_sha256": sealed_open["sha256"],
        "plan_sha256": plan["sha256"],
        "reason": reason,
        "recipe": current_book().recipe,
    }


def same_correction(existing: dict, body: dict) -> bool:
    """True when ``body`` would seal the same line ``existing`` already is."""
    old = {key: value for key, value in existing.items() if key != "sha256"}
    return canonical_bytes(old) == canonical_bytes(body)


def decide_open_correction(
    records: list[dict],
    session: str,
    bars: dict,
    fees: dict,
    index: dict[str, int],
    reason: str = STALE_OPEN_REASON,
    approved_by: str = STALE_OPEN_APPROVED_BY,
) -> tuple[list[dict], str | None]:
    """The correction to append, or nothing when that correction is already sealed.

    ``None`` as the reason means a clean no-op. A reason means the correction
    cannot be written. The body is the sizing code's open fill at the stored
    session opens, not a hand-computed share count.
    """
    plan = plan_on(records, session)
    if plan is None:
        return [], f"no plan for {session}"
    sealed = kind_on(records, session, "open_fill")
    if sealed is None:
        return [], f"no open fill for {session}"
    if any(row["kind"] in ("fill", "mark") and row["date"] == session for row in records):
        return [], f"{session} is already closed"
    if any(
        row.get("date", "") > session and row.get("kind") in ("plan", "session", "fill", "mark", "open_fill")
        for row in records
    ):
        return [], f"correction after a later book record {session}"
    state = book_state_before(records, session)
    opens = correction_opens(plan, set(state["pos"]), bars)
    corrected = open_fill_body(plan, state, bars, fees, index, opens)
    _fills_match_stored(corrected, bars)
    require_nonnegative_cash(corrected["cash_primary"], corrected["recipe"])
    body = correction_body(plan, sealed, corrected, reason, approved_by)
    latest = latest_open_correction(records, session)
    if latest is not None and same_correction(latest, body):
        return [], None
    return [body], None


def filled_symbols(records: list[dict]) -> list[tuple[str, str, str]]:
    """(session, ticker, side) for every sealed fill. A duplicate is a bug.

    A corrected open fill counts once, from the latest correction. The sealed
    line is not counted again.
    """
    latest = {}
    for record in records:
        if record.get("kind") == "open_fill_correction":
            latest[record["date"]] = record
    found = []
    for record in records:
        kind = record.get("kind")
        if kind == "open_fill" and record["date"] in latest:
            continue
        if kind == "open_fill_correction":
            if latest.get(record["date"]) is not record:
                continue
            source = record.get("corrected") or {}
            for row in source.get("buys") or []:
                found.append((record["date"], row["ticker"], "buy"))
            for row in source.get("sells") or []:
                found.append((record["date"], row["ticker"], "sell"))
            continue
        if kind in ("session", "fill", "open_fill"):
            for row in record.get("buys") or []:
                found.append((record["date"], row["ticker"], "buy"))
            for row in record.get("sells") or []:
                found.append((record["date"], row["ticker"], "sell"))
        elif kind == "mark":
            for row in record.get("added_buys") or []:
                found.append((record["date"], row["ticker"], "buy"))
            for row in record.get("added_sells") or []:
                found.append((record["date"], row["ticker"], "sell"))
    return found


def last_book_date(records: list[dict]) -> str | None:
    dates = book_dates(records)
    return dates[-1] if dates else None
