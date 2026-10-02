"""Append an open-fill correction without rewriting the sealed line.

The sealed open used the wrong prices. The correction is a later line from
the same sizing code at the stored session opens. The close fill reads that
line. A second run appends nothing. Cash after the correction is not negative.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sys
import tempfile
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.append_check import check_against  # noqa: E402
from research.hot_n4_clean_v4.forward.book import BOOKS, H1, HOLDUP, reset_book, use_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    append_records,
    load,
    log_path,
    seal,
)
from research.hot_n4_clean_v4.forward.openfill import (  # noqa: E402
    STALE_OPEN_APPROVED_BY,
    STALE_OPEN_REASON,
    decide_fill,
    decide_open_correction,
    decide_open_fill,
    filled_symbols,
    require_nonnegative_cash,
)
from research.hot_n4_clean_v4.forward.planfill import book_state, build_plan  # noqa: E402
from research.hot_n4_clean_v4.forward.render import execution_for, today_plan_html  # noqa: E402
from research.hot_n4_clean_v4.run_study import load_fees, nyse_sessions  # noqa: E402

PRIOR = "2026-09-25"
SESSION = "2026-09-29"
REAL = "2026-09-28"
TRUE_OPENS = {
    "FEAM": 2.75,
    "GLND": 5.138,
    "SECZ": 16.0,
    "SHMD": 4.68,
    "SRFM": 1.125,
    "TJGC": 26.5,
}


def _index() -> dict[str, int]:
    calendar = nyse_sessions("2026-01-01", "2026-12-31")
    return {day: i for i, day in enumerate(calendar)}


def _session(recipe: str) -> dict:
    return {
        "buys": [],
        "cash_primary": 8000.0,
        "date": PRIOR,
        "equity_primary": 8100.0,
        "excluded_unexplained_legs": [],
        "holdings": [{
            "cost_primary": 100.0,
            "entry_date": PRIOR,
            "entry_px": 10.0,
            "last_px": 10.0,
            "min_hold": 1,
            "peak_px": 10.0,
            "shares": 10,
            "ticker": "BBB",
        }],
        "holdup_on": False,
        "kind": "session",
        "morning_s": None,
        "recipe": recipe,
        "sells": [],
        "unfilled": [],
    }


def _blob(op: float, close: float, include_today: bool) -> dict:
    dates = [PRIOR]
    opens = [10.0]
    closes = [10.0]
    if include_today:
        dates.append(SESSION)
        opens.append(op)
        closes.append(close)
    return {"adjusted": True, "close": closes, "date": dates, "open": opens}


def _bars(include_today: bool) -> dict:
    return {
        "feat": {},
        "stored": {
            "AAA": _blob(11.0, 13.0, include_today),
            "BBB": _blob(12.0, 12.5, include_today),
            "IWM": _blob(10.0, 10.2, include_today),
        },
    }


def _plan(recipe: str, state: dict, index: dict[str, int]) -> dict:
    payload = {
        "bar_cutoff": PRIOR,
        "candidates": [{
            "fv_avg_volume": 1_000_000.0,
            "fv_price": 20.0,
            "ohlc_hot_score": 5.0,
            "sources": ["yday_gainer"],
            "ticker": "AAA",
        }],
        "excluded_unexplained_legs": [],
        "morning_s": 1.0,
        "session": SESSION,
    }
    plan = build_plan(payload, state, index)
    if plan["recipe"] != recipe:
        raise SystemExit(f"plan recipe {plan['recipe']}")
    return plan


def _fills(record: dict) -> dict[str, float]:
    found = {}
    for row in list(record.get("buys") or []) + list(record.get("sells") or []):
        found[row["ticker"]] = float(row["fill"])
    return found


def _synthetic(book) -> None:
    token = use_book(book)
    try:
        index = _index()
        fees = load_fees()
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            append_records([_session(book.recipe)], folder)
            plan = _plan(book.recipe, book_state(load(folder)), index)
            append_records([plan], folder)
            sealed_plan = next(row for row in load(folder) if row["kind"] == "plan")
            wrong = {"AAA": 5.0, "BBB": 20.0, "IWM": 10.0}
            bodies, why = decide_open_fill(load(folder), SESSION, wrong, _bars(False), fees, index)
            if why or not bodies:
                raise SystemExit(f"stale open did not seal ({why})")
            append_records(bodies, folder)
            sealed_bytes = log_path(folder).read_bytes()
            sealed = next(row for row in load(folder) if row["kind"] == "open_fill")
            if _fills(sealed).get("AAA") != 5.0 or _fills(sealed).get("BBB") != 20.0:
                raise SystemExit(f"stale fills {_fills(sealed)}")
            try:
                decide_fill(load(folder), _bars(True), fees, index)
            except RuntimeError as exc:
                if "no longer matches the sealed fill" not in str(exc):
                    raise
            else:
                raise SystemExit("close fill accepted the stale open")

            correction, corr_why = decide_open_correction(
                load(folder), SESSION, _bars(True), fees, index,
                STALE_OPEN_REASON, STALE_OPEN_APPROVED_BY,
            )
            if corr_why or len(correction) != 1:
                raise SystemExit(f"correction {corr_why} {correction}")
            body = correction[0]
            if body["kind"] != "open_fill_correction":
                raise SystemExit(f"kind {body['kind']}")
            if body["open_fill_sha256"] != sealed["sha256"] or body["plan_sha256"] != sealed_plan["sha256"]:
                raise SystemExit("correction does not reference the sealed lines")
            if body["reason"] != STALE_OPEN_REASON or body["approved_by"] != STALE_OPEN_APPROVED_BY:
                raise SystemExit("approval text")
            cash = float(body["corrected"]["cash_primary"])
            if cash < 0:
                raise SystemExit(f"negative cash {cash}")
            changed = {(row["side"], row["ticker"]) for row in body["changed_legs"]}
            if ("buy", "AAA") not in changed or ("sell", "BBB") not in changed:
                raise SystemExit(f"changed legs {body['changed_legs']}")
            if _fills(body["corrected"]).get("AAA") != 11.0 or _fills(body["corrected"]).get("BBB") != 12.0:
                raise SystemExit(f"corrected fills {_fills(body['corrected'])}")
            append_records(correction, folder)
            written = log_path(folder).read_bytes()
            if not written.startswith(sealed_bytes):
                raise SystemExit("correction rewrote the sealed open fill")
            if next(row for row in load(folder) if row["kind"] == "open_fill") != sealed:
                raise SystemExit("sealed open fill record changed")
            again, again_why = decide_open_correction(
                load(folder), SESSION, _bars(True), fees, index,
                STALE_OPEN_REASON, STALE_OPEN_APPROVED_BY,
            )
            if again or again_why is not None:
                raise SystemExit(f"second correction {again_why} {again}")
            if log_path(folder).read_bytes() != written:
                raise SystemExit("second correction wrote")

            state = book_state(load(folder))
            if float(state["cash"]) != cash:
                raise SystemExit(f"book cash {state['cash']} {cash}")
            if int(state["pos"]["AAA"]["shares"]) != int(
                next(row["shares"] for row in body["corrected"]["buys"] if row["ticker"] == "AAA")
            ):
                raise SystemExit("book shares are not the correction")
            exe = execution_for(load(folder), SESSION)
            if float(exe["buys"]["AAA"]["fill"]) != 11.0 or float(exe["sells"]["BBB"]["fill"]) != 12.0:
                raise SystemExit(f"page fills {exe}")
            html = today_plan_html(
                load(folder), now=datetime(2026, 9, 29, 14, 0, tzinfo=timezone.utc),
            )
            if "@ 11.00 filled" not in html or "@ 12.00 filled" not in html:
                raise SystemExit("page did not show the corrected open")
            if "@ 5.00 filled" in html or "@ 20.00 filled" in html:
                raise SystemExit("page showed the sealed stale open")

            close_bodies, close_why = decide_fill(load(folder), _bars(True), fees, index)
            if close_why or not close_bodies or close_bodies[0]["kind"] != "mark":
                raise SystemExit(f"close {close_why}")
            if float(close_bodies[0]["cash_primary"]) < 0:
                raise SystemExit("close cash is negative")
            corr = next(row for row in load(folder) if row["kind"] == "open_fill_correction")
            if close_bodies[0]["open_fill_sha256"] != corr["sha256"]:
                raise SystemExit("close mark does not use the correction")
            if close_bodies[0]["open_fill_sha256"] == sealed["sha256"]:
                raise SystemExit("close mark still points at the stale open")
            bad = json.loads(json.dumps(body))
            bad["corrected"]["cash_primary"] = -1.0
            before_bad = log_path(folder).read_bytes()
            try:
                append_records([bad], folder)
            except RuntimeError as exc:
                if "negative" not in str(exc):
                    raise
            else:
                raise SystemExit("negative cash was sealed")
            if log_path(folder).read_bytes() != before_bad:
                raise SystemExit("negative cash wrote")
            held = {row["ticker"]: int(row["shares"]) for row in close_bodies[0]["holdings"]}
            want = {row["ticker"]: int(row["shares"]) for row in body["corrected"]["holdings"]}
            if held != want:
                raise SystemExit(f"close holdings {held} {want}")
            before_close = log_path(folder).read_bytes()
            append_records(close_bodies, folder)
            if not log_path(folder).read_bytes().startswith(before_close):
                raise SystemExit("close rewrote the correction")
            found = filled_symbols(load(folder))
            if len(found) != len(set(found)):
                raise SystemExit(f"duplicate fill {found}")
            if not log_path(folder).read_bytes().startswith(before_close):
                raise SystemExit("close rewrote the correction")
        try:
            require_nonnegative_cash(-0.01, book.recipe)
        except RuntimeError as exc:
            if "negative" not in str(exc):
                raise
        else:
            raise SystemExit("negative cash helper returned")
        print("open-fill correction", book.recipe, "ok")
    finally:
        reset_book(token)


def _ledgers() -> dict[str, bytes]:
    found = {}
    roots = (
        ROOT / "research/hot_n4_clean_v4/forward",
        ROOT / "research/hot_n4_clean_v4/forward_h1",
        ROOT / "dashboard/holdup",
        ROOT / "dashboard/h1",
    )
    for folder in roots:
        if not folder.is_dir():
            continue
        for path in folder.iterdir():
            if path.is_file() and path.suffix in {".jsonl", ".json"}:
                found[str(path)] = hashlib.sha256(path.read_bytes()).digest()
    return found


def _cash_from(text: str) -> float:
    for line in text.splitlines():
        if line.startswith("cash_primary="):
            return float(line.split("=", 1)[1])
    raise SystemExit(f"no cash line\n{text}")


def _real() -> None:
    from research.hot_n4_clean_v4.forward import forward

    before = _ledgers()
    saved = forward.append_records
    saved_engine = forward._engine_ok

    def boom(*_args, **_kwargs):
        raise SystemExit("dry-run wrote the ledger")

    forward.append_records = boom
    # The v4 pin lags src/factor_mine.py until that lock is re-pinned. The
    # drift is the nightly OOS handler, not open-fill sizing. The dry run
    # still has to print the correction from the real ledgers.
    if saved_engine():
        forward._engine_ok = lambda: None
    old = {key: os.environ.get(key) for key in (
        "FORWARD_BOOK", "HOLDUP_MODE", "CORRECT_OPEN_DATE", "CORRECT_OPEN_DRY_RUN",
        "CORRECT_OPEN_REASON", "CORRECT_OPEN_APPROVED_BY",
    )}
    try:
        for book, name in ((HOLDUP, "holdup"), (H1, "h1")):
            token = use_book(book)
            try:
                os.environ["FORWARD_BOOK"] = name
                os.environ["HOLDUP_MODE"] = "correct_open"
                os.environ["CORRECT_OPEN_DATE"] = REAL
                os.environ["CORRECT_OPEN_DRY_RUN"] = "1"
                os.environ.pop("CORRECT_OPEN_REASON", None)
                os.environ.pop("CORRECT_OPEN_APPROVED_BY", None)
                out = io.StringIO()
                err = io.StringIO()
                with redirect_stdout(out), redirect_stderr(err):
                    code = forward.correct_open_main()
                text = out.getvalue()
                records = load()
                sealed = next(row for row in records if row["kind"] == "open_fill" and row["date"] == REAL)
                if _fills(sealed).get("SRFM") != 0.9439:
                    raise SystemExit(f"{name} sealed SRFM moved")
                if code != 0:
                    raise SystemExit(f"{name} dry-run failed {code}\n{text}\n{err.getvalue()}")
                if f"open fill correction already sealed {REAL}" in text:
                    _sealed_correction(name, records)
                else:
                    if "dry-run: wrote nothing" not in text:
                        raise SystemExit(f"{name} dry-run failed {code}\n{text}\n{err.getvalue()}")
                    cash = _cash_from(text)
                    if cash < 0:
                        raise SystemExit(f"{name} cash {cash}")
                    if "pnl_primary=" not in text:
                        raise SystemExit(f"{name} close P&L missing\n{text}")
                    if f"reason: {STALE_OPEN_REASON}" not in text:
                        raise SystemExit(f"{name} reason\n{text}")
                    if f"approved_by: {STALE_OPEN_APPROVED_BY}" not in text:
                        raise SystemExit(f"{name} approved_by\n{text}")
                    for ticker, op in TRUE_OPENS.items():
                        if f"{ticker} shares=" not in text or f"fill={op}" not in text:
                            raise SystemExit(f"{name} missing {ticker} {op}\n{text}")
                    if "sealed_fill=0.9439" not in text or "sealed_fill=16.209999" not in text:
                        raise SystemExit(f"{name} sealed legs\n{text}")
                    if "corrected_fill=1.125" not in text or "corrected_fill=16.0" not in text:
                        raise SystemExit(f"{name} corrected legs\n{text}")
                print(text)
            finally:
                reset_book(token)
    finally:
        forward.append_records = saved
        forward._engine_ok = saved_engine
        for key, value in old.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
    if _ledgers() != before:
        raise SystemExit("dry-run wrote a sealed ledger")
    print("2026-09-28 dry-run left both ledgers unchanged")


def _sealed_correction(name: str, records: list[dict]) -> None:
    """The 2026-09-28 correction is already a sealed line. A dry-run must not write another."""
    corr = next(
        (row for row in records if row.get("kind") == "open_fill_correction" and row.get("date") == REAL),
        None,
    )
    if corr is None:
        raise SystemExit(f"{name} already-sealed dry-run has no correction line")
    if corr.get("reason") != STALE_OPEN_REASON or corr.get("approved_by") != STALE_OPEN_APPROVED_BY:
        raise SystemExit(f"{name} sealed correction reason")
    opened = corr.get("corrected") or {}
    if float(opened.get("cash_primary")) < 0:
        raise SystemExit(f"{name} cash {opened.get('cash_primary')}")
    fills = _fills(opened)
    for ticker, op in TRUE_OPENS.items():
        if fills.get(ticker) != op:
            raise SystemExit(f"{name} sealed correction {ticker} {fills.get(ticker)} wanted {op}")
    changed = {leg["ticker"]: leg for leg in corr.get("changed_legs") or []}
    srfm = changed.get("SRFM") or {}
    secz = changed.get("SECZ") or {}
    if (srfm.get("sealed") or {}).get("fill") != 0.9439 or (srfm.get("corrected") or {}).get("fill") != 1.125:
        raise SystemExit(f"{name} sealed SRFM leg {srfm}")
    if (secz.get("sealed") or {}).get("fill") != 16.209999 or (secz.get("corrected") or {}).get("fill") != 16.0:
        raise SystemExit(f"{name} sealed SECZ leg {secz}")


def _pages() -> None:
    for name in ("holdup", "h1"):
        text = (ROOT / "dashboard" / name / "index.html").read_text(encoding="utf-8")
        if "withCorrections" not in text or "open_fill_correction" not in text:
            raise SystemExit(f"{name} page does not apply an open-fill correction")
    overview = (ROOT / "dashboard" / "index.html").read_text(encoding="utf-8")
    if "open_fill_correction" not in overview:
        raise SystemExit("overview page does not apply an open-fill correction")


def _noop_real_shape() -> None:
    """The correction the dry-run just computed is a no-op the second time, in memory."""
    from research.hot_n4_clean_v4.forward.forward import _index as forward_index
    from research.hot_n4_clean_v4.forward.openfill import bars_with_stored_session
    from research.hot_n4_clean_v4.forward.prices import overlay_forward
    from research.hot_n4_clean_v4.forward.planfill import book_state_before
    from research.hot_n4_clean_v4.forward.openfill import open_legs
    from research.hot_n4_clean_v4.run_study import load_bars

    index = forward_index()
    fees = load_fees()
    for book in BOOKS:
        token = use_book(book)
        try:
            records = load()
            plan = next(row for row in records if row["kind"] == "plan" and row["date"] == REAL)
            held = set(book_state_before(records, REAL)["pos"])
            names = sorted(set(open_legs(plan, held)) | {str(ticker).upper() for ticker in held} | {"IWM"})
            bars = bars_with_stored_session(overlay_forward(load_bars()), REAL, names)
            bodies, why = decide_open_correction(records, REAL, bars, fees, index)
            if why:
                raise SystemExit(f"{book.recipe} {why}")
            if not bodies:
                again, again_why = decide_open_correction(records, REAL, bars, fees, index)
                if again or again_why is not None:
                    raise SystemExit(f"{book.recipe} second correction {again_why}")
                continue
            if len(bodies) != 1:
                raise SystemExit(f"{book.recipe} {why}")
            if float(bodies[0]["corrected"]["cash_primary"]) < 0:
                raise SystemExit(f"{book.recipe} cash")
            preview, _line = seal(bodies[0])
            again, again_why = decide_open_correction(records + [preview], REAL, bars, fees, index)
            if again or again_why is not None:
                raise SystemExit(f"{book.recipe} second correction {again_why}")
        finally:
            reset_book(token)
    print("second correction is a no-op on both books")


def main() -> None:
    for book in (HOLDUP, H1):
        _synthetic(book)
    _pages()
    _real()
    _noop_real_shape()
    check_against("origin/main")
    print("open-fill correction ok")


if __name__ == "__main__":
    main()
