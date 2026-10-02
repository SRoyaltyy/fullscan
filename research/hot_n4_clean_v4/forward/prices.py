"""Append new Yahoo sessions for the holdup book. Never edit a stored bar.

The pinned v4 file ``data/prices/ohlc.parquet`` is read-only here. New
sessions after 2026-09-25 go to ``prices.jsonl``. Each fetch is one line in
``PRICE_LEDGER.jsonl``, hashed with sha256. A Yahoo print that disagrees
with a bar already in ``prices.jsonl`` is written to ``price_revisions.jsonl``
and is not copied over the stored bar. A sealed bar whose only changes are
volume, or OHLC fields that moved by at most one cent, does not stop the
run. One sealed OHLC field that moved by more than one cent stays pending:
the stored bar is left as it is, the ledger names that leg, and new bars
still append. More than one such field stops the run and appends nothing.

Yahoo is asked for split-adjusted daily bars with ``auto_adjust=False`` and
``actions=True``, the same call ``src/price_store.py`` locks: dividends are
not applied. A new >3x leg is classified with the locked split table and
the filing-verified real-move table. An unexplained leg removes a candidate.
The same leg on a held name, or any non-split leg on IWM, halts and appends
nothing.
"""
from __future__ import annotations

import json
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import current_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    HERE,
    book_dates,
    canonical_bytes,
    load,
    open_plan,
    sha256_bytes,
)
from research.hot_n4_clean_v4.forward.planfill import book_state  # noqa: E402
from research.hot_n4_clean_v4.run_study import (  # noqa: E402
    Halt,
    classify,
    liquid_universe,
    removal_legs,
    scan_legs,
)
from src.price_store import AUTO_ADJUST, _flatten_actions, _flatten_yf  # noqa: E402

PIN_END = "2026-09-25"
# 21:15 UTC is after the US cash close in both EDT (20:00 UTC) and EST (21:00 UTC).
FINAL_UTC = time(21, 15)
PRICES_NAME = "prices.jsonl"
LEDGER_NAME = "PRICE_LEDGER.jsonl"
REVISIONS_NAME = "price_revisions.jsonl"


class SealedBarRevision(RuntimeError):
    """Yahoo changed a bar a sealed record already used."""


def _price_folder(folder: Path | None) -> Path:
    return folder or current_book().folder


def prices_path(folder: Path | None = None) -> Path:
    return _price_folder(folder) / PRICES_NAME


def price_ledger_path(folder: Path | None = None) -> Path:
    return _price_folder(folder) / LEDGER_NAME


def revisions_path(folder: Path | None = None) -> Path:
    return _price_folder(folder) / REVISIONS_NAME


def bar_is_final(session: str, now: datetime) -> bool:
    """A daily bar is final once 21:15 UTC on that session has passed."""
    when = now if now.tzinfo else now.replace(tzinfo=timezone.utc)
    final_at = datetime.fromisoformat(f"{session}T{FINAL_UTC.isoformat()}+00:00")
    return when.astimezone(timezone.utc) >= final_at


def _round_bar(row: dict) -> dict:
    return {
        "close": round(float(row["close"]), 6),
        "date": str(row["date"])[:10],
        "high": round(float(row["high"]), 6),
        "low": round(float(row["low"]), 6),
        "open": round(float(row["open"]), 6),
        "ticker": str(row["ticker"]).upper(),
        "volume": float(round(float(row["volume"]))),
    }


def _same(old: dict, new: dict) -> bool:
    for key in ("open", "high", "low", "close", "volume"):
        if float(old[key]) != float(new[key]):
            return False
    return True


def _over_cent(old: float, new: float) -> bool:
    """True when an OHLC print moved by more than one cent.

    Stored prices are rounded to 6 decimals. A one-cent gap can be a hair
    over 0.01 in binary, so the test uses that same 6-decimal gap.
    """
    return round(abs(float(new) - float(old)), 6) > 0.01


def _material_ohlc(old: dict, new: dict) -> list[dict]:
    """Sealed OHLC fields on one bar that moved by more than one cent."""
    legs = []
    for field in ("open", "high", "low", "close"):
        old_v = float(old[field])
        new_v = float(new[field])
        if _over_cent(old_v, new_v):
            legs.append({
                "field": field,
                "new": new_v,
                "old": old_v,
                "ticker": str(new["ticker"]),
            })
    return legs


def load_price_rows(folder: Path | None = None) -> list[dict]:
    path = prices_path(folder)
    if not path.is_file() or path.stat().st_size == 0:
        return []
    text = path.read_bytes()
    if b"\r" in text or not text.endswith(b"\n"):
        raise RuntimeError("prices.jsonl")
    rows = []
    seen = set()
    for line in text.splitlines():
        if not line:
            raise RuntimeError("blank price line")
        row = _round_bar(json.loads(line))
        key = (row["ticker"], row["date"])
        if key in seen:
            raise RuntimeError(f"duplicate stored bar {key}")
        if row["date"] <= PIN_END:
            raise RuntimeError(f"forward store has a pinned date {row['date']}")
        seen.add(key)
        rows.append(row)
    return rows


def _append_lines(path: Path, lines: list[bytes]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    before = path.read_bytes() if path.is_file() else b""
    with path.open("ab") as handle:
        for line in lines:
            handle.write(line)
    if not path.read_bytes().startswith(before):
        raise RuntimeError(f"{path.name} prefix changed")


def sealed_bar_keys(records: list[dict]) -> set[tuple[str, str]]:
    """(ticker, session) whose open or close a sealed record used."""
    keys = set()
    for record in records:
        kind = record.get("kind")
        if kind in ("session", "fill", "open_fill"):
            for row in list(record.get("buys") or []) + list(record.get("sells") or []):
                keys.add((row["ticker"], record["date"]))
            for row in record.get("holdings") or []:
                keys.add((row["ticker"], record["date"]))
        elif kind == "open_fill_correction":
            source = record.get("corrected") or {}
            for row in list(source.get("buys") or []) + list(source.get("sells") or []):
                keys.add((row["ticker"], record["date"]))
            for row in source.get("holdings") or []:
                keys.add((row["ticker"], record["date"]))
        elif kind == "mark":
            for row in list(record.get("added_buys") or []) + list(record.get("added_sells") or []):
                keys.add((row["ticker"], record["date"]))
            for row in record.get("holdings") or []:
                keys.add((row["ticker"], record["date"]))
        elif kind == "close":
            keys.add((record["ticker"], record["date"]))
    return keys


def overlay_rows(bars: dict, rows: list[dict]) -> dict:
    """Return bars with forward sessions appended. Does not mutate ``bars``."""
    import numpy as np

    out = {"feat": {}, "stored": {}}
    for side in ("feat", "stored"):
        for ticker, blob in bars[side].items():
            copied = {}
            for key, value in blob.items():
                if key == "date":
                    copied[key] = list(value)
                elif key == "adjusted":
                    copied[key] = value
                else:
                    copied[key] = np.array(value, dtype=float, copy=True)
            out[side][ticker] = copied
    by_ticker: dict[str, list[dict]] = {}
    for row in rows:
        if row["date"] <= PIN_END:
            raise RuntimeError(f"refusing to overlay a pinned date {row['date']}")
        by_ticker.setdefault(row["ticker"], []).append(row)
    for ticker, extra in by_ticker.items():
        extra.sort(key=lambda item: item["date"])
        for side, fields in (
            ("stored", ("open", "close")),
            ("feat", ("open", "high", "low", "close", "volume")),
        ):
            blob = out[side].get(ticker)
            if blob is None:
                blob = {"date": [], "adjusted": True}
                for field in fields:
                    blob[field] = np.array([], dtype=float)
                out[side][ticker] = blob
            dates = list(blob["date"])
            for row in extra:
                if dates and row["date"] <= dates[-1]:
                    if row["date"] in dates:
                        continue
                    raise RuntimeError(f"forward bar out of order {ticker} {row['date']}")
                dates.append(row["date"])
                for field in fields:
                    blob[field] = np.concatenate([
                        np.asarray(blob[field], dtype=float),
                        np.asarray([float(row[field])], dtype=float),
                    ])
            blob["date"] = dates
    return out


def overlay_forward(bars: dict, folder: Path | None = None) -> dict:
    return overlay_rows(bars, load_price_rows(folder))


def _stored_view(pinned: dict, rows: list[dict]) -> dict[str, dict]:
    view = {}
    tickers = set(pinned) | {row["ticker"] for row in rows}
    extra: dict[str, list[dict]] = {}
    for row in rows:
        extra.setdefault(row["ticker"], []).append(row)
    for ticker in tickers:
        blob = pinned.get(ticker)
        dates = list(blob["date"]) if blob else []
        opens = [float(x) for x in blob["open"]] if blob else []
        closes = [float(x) for x in blob["close"]] if blob else []
        for row in sorted(extra.get(ticker, []), key=lambda item: item["date"]):
            if dates and row["date"] <= dates[-1]:
                continue
            dates.append(row["date"])
            opens.append(float(row["open"]))
            closes.append(float(row["close"]))
        if dates:
            view[ticker] = {"date": dates, "open": opens, "close": closes}
    return view


def _with_bar(view: dict, row: dict) -> dict:
    ticker = row["ticker"]
    blob = view.get(ticker)
    dates = list(blob["date"]) if blob else []
    opens = list(blob["open"]) if blob else []
    closes = list(blob["close"]) if blob else []
    if dates and row["date"] <= dates[-1]:
        return view
    dates.append(row["date"])
    opens.append(float(row["open"]))
    closes.append(float(row["close"]))
    out = dict(view)
    out[ticker] = {"date": dates, "open": opens, "close": closes}
    return out


def new_leg_action(view: dict, row: dict, held: set[str]) -> tuple[str, list[dict]]:
    """Return ('ok', []), ('exclude', legs), or ('halt', legs) for one new bar."""
    ticker = row["ticker"]
    session = row["date"]
    scanned = _with_bar(view, row)
    if ticker == "IWM":
        legs = [
            leg for leg in scan_legs(scanned, ticker, session)
            if leg["bar_date"] == session and classify(leg) != "split"
        ]
        if legs:
            return "halt", legs
        return "ok", []
    legs = [
        leg for leg in removal_legs(scanned, ticker, session)
        if leg["bar_date"] == session
    ]
    halt = [leg for leg in legs if leg["leg"] in ("open_over_prev_close", "close_over_open")]
    if ticker in held:
        if halt:
            return "halt", halt
        return "ok", []
    if not legs:
        return "ok", []
    return "exclude", legs


def fetch_yahoo(tickers: list[str], start: str, end: str) -> dict:
    """Split-adjusted Yahoo daily bars. ``end`` is exclusive. Missing names are listed."""
    from research.hot_n4_clean_v4.forward.opens import ny_bar_date

    if AUTO_ADJUST:
        raise RuntimeError("auto_adjust must stay false so dividends are not applied")
    try:
        import yfinance as yf
    except ImportError as exc:
        return {"bars": [], "error": f"yfinance is not installed ({exc})", "missing": list(tickers), "splits": []}
    bars = []
    splits = []
    missing = []
    pending = []
    seen = set()
    for ticker in tickers:
        name = str(ticker).strip().upper()
        if name and name not in seen:
            seen.add(name)
            pending.append(name)
    batch = 40
    while pending:
        chunk = pending[:batch]
        pending = pending[batch:]
        try:
            raw = yf.download(
                tickers=chunk, start=start, end=end, group_by="ticker",
                auto_adjust=False, actions=True, threads=False, progress=False,
                repair=False,
            )
        except Exception as exc:  # noqa: BLE001 — a failed download appends nothing
            if len(chunk) == 1:
                missing.append({"reason": str(exc), "ticker": chunk[0]})
                continue
            pending = chunk + pending
            batch = max(1, batch // 2)
            continue
        if raw is None or getattr(raw, "empty", True):
            missing.extend({"reason": "no bars", "ticker": ticker} for ticker in chunk)
            continue
        flat = _flatten_yf(raw, chunk)
        got = set()
        if flat is not None and len(flat):
            for item in flat.itertuples(index=False):
                opx = float(item.open) if item.open == item.open else 0.0
                high = float(item.high) if item.high == item.high else opx
                low = float(item.low) if item.low == item.low else opx
                close = float(item.close) if item.close == item.close else 0.0
                volume = float(item.volume) if item.volume == item.volume else 0.0
                if opx <= 0 or close <= 0:
                    continue
                ticker = str(item.ticker).upper()
                got.add(ticker)
                when = ny_bar_date(item.date)
                if when is None:
                    continue
                bars.append(_round_bar({
                    "close": close,
                    "date": when,
                    "high": high,
                    "low": low,
                    "open": opx,
                    "ticker": ticker,
                    "volume": volume,
                }))
        try:
            actions = _flatten_actions(raw, chunk)
        except Exception as exc:  # noqa: BLE001
            actions = None
            missing.append({"reason": f"split extract failed ({exc})", "ticker": chunk[0]})
        if actions is not None and len(actions):
            for item in actions.itertuples(index=False):
                factor = float(item.split) if item.split == item.split else 0.0
                if factor <= 0 or abs(factor - 1.0) <= 1e-12:
                    continue
                splits.append({
                    "date": str(item.date)[:10],
                    "split": round(factor, 8),
                    "ticker": str(item.ticker).upper(),
                })
        for ticker in chunk:
            if ticker not in got:
                missing.append({"reason": "no bars", "ticker": ticker})
    splits.sort(key=lambda row: (row["ticker"], row["date"], row["split"]))
    bars.sort(key=lambda row: (row["ticker"], row["date"]))
    return {"bars": bars, "error": None, "missing": missing, "splits": splits}


def _ledger_line(body: dict) -> bytes:
    return canonical_bytes(body)


def _write_ledger(folder: Path, body: dict) -> None:
    _append_lines(price_ledger_path(folder), [_ledger_line(body)])


def refresh(
    *,
    tickers: list[str],
    session: str,
    held: set[str],
    pinned_stored: dict,
    records: list[dict],
    fetch,
    folder: Path,
    now: datetime | None = None,
    note: str | None = None,
) -> dict:
    """Fetch ``tickers`` and append only new final sessions under ``folder``.

    Returns the ledger body. Raises Halt or SealedBarRevision after the
    ledger line is written. A missing name stays in ``missing`` and does
    not stop the others. A sealed volume change, or a sealed OHLC move of
    at most one cent, is logged and does not overwrite the stored bar. One
    sealed OHLC field that moved by more than one cent stays pending: the
    stored bar is left as it is, the ledger names that leg, and new bars
    still append. More than one such field raises SealedBarRevision and
    appends nothing.
    """
    now = now or datetime.now(timezone.utc)
    folder.mkdir(parents=True, exist_ok=True)
    stored_rows = load_price_rows(folder)
    if stored_rows == [] and not bar_is_final(session, now):
        payload = {
            "session": session,
            "skipped": "not-final",
            "tickers": sorted({str(ticker).upper() for ticker in tickers}),
        }
        body = {
            "appended": [],
            "at": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "excluded_unexplained_legs": [],
            "missing": [],
            "note": note,
            "revisions": 0,
            "session": session,
            "sha256": sha256_bytes(canonical_bytes(payload)),
            "why": f"{session} bar is not final until 21:15 UTC; appended nothing",
        }
        _write_ledger(folder, body)
        return body
    start = PIN_END
    if stored_rows:
        start = min(start, min(row["date"] for row in stored_rows))
    end = (date.fromisoformat(session) + timedelta(days=1)).isoformat()
    fetched = fetch(tickers, start, end)
    payload = {
        "bars": fetched.get("bars") or [],
        "session": session,
        "splits": fetched.get("splits") or [],
        "tickers": sorted({str(t).upper() for t in tickers}),
    }
    digest = sha256_bytes(canonical_bytes(payload))
    error = fetched.get("error")
    if error:
        body = {
            "appended": [],
            "at": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "excluded_unexplained_legs": [],
            "missing": fetched.get("missing") or [],
            "note": note,
            "revisions": 0,
            "session": session,
            "sha256": digest,
            "why": error,
        }
        _write_ledger(folder, body)
        return body
    known = {(row["ticker"], row["date"]): row for row in stored_rows}
    revisions = []
    fresh = []
    not_final = []
    for row in payload["bars"]:
        if row["date"] <= PIN_END or row["date"] > session:
            continue
        if not bar_is_final(row["date"], now):
            not_final.append({"date": row["date"], "ticker": row["ticker"]})
            continue
        old = known.get((row["ticker"], row["date"]))
        if old is None:
            fresh.append(row)
            continue
        if not _same(old, row):
            revisions.append({"new": row, "old": old})
    used = sealed_bar_keys(records)
    revision_lines = []
    material: list[dict] = []
    for rev in revisions:
        new = rev["new"]
        key = (new["ticker"], new["date"])
        touched = key in used
        revision_lines.append(canonical_bytes({
            "at": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "date": new["date"],
            "new": new,
            "old": rev["old"],
            "sealed_use": touched,
            "ticker": new["ticker"],
        }))
        if touched:
            material.extend(_material_ohlc(rev["old"], new))
    if revision_lines:
        _append_lines(revisions_path(folder), revision_lines)
    # More than one sealed OHLC field moved by more than one cent. That is
    # not a single pending leg, so the session appends nothing.
    if len(material) > 1:
        body = {
            "appended": [],
            "at": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "excluded_unexplained_legs": [],
            "missing": fetched.get("missing") or [],
            "note": note,
            "revisions": len(revisions),
            "session": session,
            "sha256": digest,
            "why": "Yahoo revised a bar a sealed record used",
        }
        _write_ledger(folder, body)
        raise SealedBarRevision(body["why"])
    pending = material[0] if material else None
    view = _stored_view(pinned_stored, stored_rows)
    excluded = []
    halted = []
    accepted = []
    for row in fresh:
        action, legs = new_leg_action(view, row, held)
        if action == "halt":
            halted.extend(legs)
            continue
        if action == "exclude":
            excluded.extend(legs)
        accepted.append(row)
        view = _with_bar(view, row)
    if halted:
        body = {
            "appended": [],
            "at": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "excluded_unexplained_legs": excluded,
            "halted": halted,
            "missing": fetched.get("missing") or [],
            "note": note,
            "revisions": len(revisions),
            "session": session,
            "sha256": digest,
            "why": (
                f"halt {halted[0]['ticker']} {halted[0]['leg']} on {halted[0]['bar_date']}"
            ),
        }
        _write_ledger(folder, body)
        raise Halt(body["why"])
    accepted.sort(key=lambda row: (row["ticker"], row["date"]))
    if accepted:
        _append_lines(prices_path(folder), [canonical_bytes(row) for row in accepted])
    why = None
    if not accepted:
        if not_final:
            why = f"{session} bar is not final until 21:15 UTC; appended nothing"
        elif fetched.get("missing") and not payload["bars"]:
            why = "Yahoo returned no bars; appended nothing"
        else:
            why = f"no new session after {PIN_END}; appended nothing"
    if pending:
        named = (
            f"pending {pending['ticker']} {pending['field']} "
            f"old {pending['old']} new {pending['new']}"
        )
        why = f"{named}; {why}" if why else named
    body = {
        "appended": [{"date": row["date"], "ticker": row["ticker"]} for row in accepted],
        "at": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "excluded_unexplained_legs": excluded,
        "missing": fetched.get("missing") or [],
        "note": note,
        "revisions": len(revisions),
        "session": session,
        "sha256": digest,
        "why": why,
    }
    if pending:
        body["pending"] = pending
    _write_ledger(folder, body)
    return body


def target_session(records: list[dict], next_session) -> str:
    pending = open_plan(records)
    if pending:
        return pending["date"]
    dates = book_dates(records)
    if not dates:
        raise RuntimeError("no sealed book")
    return next_session(dates[-1])


def main() -> int:
    from research.hot_n4_clean_v4.forward.forward import frozen_frame, next_session
    from research.hot_n4_clean_v4.run_study import load_bars

    try:
        records = load()
    except RuntimeError as exc:
        print(f"REFUSING: {exc}", file=sys.stderr)
        return 1
    try:
        session = target_session(records, next_session)
    except RuntimeError as exc:
        print(f"REFUSING: {exc}", file=sys.stderr)
        return 1
    state = book_state(records)
    held = set(state["pos"])
    frame, why = frozen_frame(session)
    note = None
    names = set(held) | {"IWM"}
    if why or frame is None:
        note = why or "candidate universe unavailable; fetching held names and IWM only"
        print(note, flush=True)
    else:
        names |= {row["ticker"] for row in liquid_universe(frame)}
    tickers = sorted(names)
    print(f"price refresh {session} tickers {len(tickers)}", flush=True)
    try:
        bars = load_bars()
    except Exception as exc:  # noqa: BLE001
        print(f"append nothing: pinned prices could not be read ({exc})")
        return 0
    try:
        body = refresh(
            tickers=tickers,
            session=session,
            held=held,
            pinned_stored=bars["stored"],
            records=records,
            fetch=fetch_yahoo,
            folder=current_book().folder,
            note=note,
        )
    except (Halt, SealedBarRevision) as exc:
        print(f"REFUSING: {exc}", file=sys.stderr)
        return 1
    print(
        f"appended {len(body['appended'])} bars"
        + (f"; {body['why']}" if body.get("why") else ""),
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except RuntimeError as exc:
        print(f"REFUSING: {exc}", file=sys.stderr)
        raise SystemExit(1)
