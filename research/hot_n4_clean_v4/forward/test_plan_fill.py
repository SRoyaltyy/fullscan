"""Plan-before-fill, idempotent reruns, sealed bars, and the 2026-09-28 dry-run."""
from __future__ import annotations

import copy
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.dry_run import simulate  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import (  # noqa: E402
    RECIPE,
    append_records,
    load,
    log_path,
    open_plan,
    seal,
)
from research.hot_n4_clean_v4.forward.prices import (  # noqa: E402
    SealedBarRevision,
    load_price_rows,
    overlay_rows,
    prices_path,
    refresh,
    revisions_path,
)
from research.hot_n4_clean_v4.run_study import Halt  # noqa: E402

SESSION = "2026-09-28"
PRIOR = "2026-09-25"


def _plan() -> dict:
    return {
        "bar_cutoff": PRIOR,
        "cash_before": 100.0,
        "date": SESSION,
        "excluded_unexplained_legs": [],
        "holdup_on": False,
        "kind": "plan",
        "morning_s": 1.0,
        "picks": [{
            "fv_avg_volume": 1000.0,
            "fv_price": 10.0,
            "rank": 1,
            "sources": ["ohlc_hot"],
            "ticker": "AAA",
        }],
        "planned_sells": [{
            "reason": "hold-expired",
            "shares": 5,
            "ticker": "BBB",
        }],
        "recipe": RECIPE,
    }


def _fill(plan_sha: str) -> tuple[dict, dict]:
    fill = {
        "buys": [{
            "fee": 0.1,
            "fill": 10.0,
            "fill_basis": "open",
            "liquidity_cap_shares": 100,
            "rank": 1,
            "shares": 1,
            "sources": ["ohlc_hot"],
            "ticker": "AAA",
        }],
        "cash_primary": 90.0,
        "cost_model": "futubull+0.5%/side",
        "date": SESSION,
        "equity_primary": 100.0,
        "excluded_on_fill": [],
        "holdings": [{
            "cost_primary": 10.0,
            "entry_date": SESSION,
            "entry_px": 10.0,
            "last_px": 10.0,
            "min_hold": 1,
            "peak_px": 10.0,
            "shares": 1,
            "ticker": "AAA",
        }],
        "holdup_on": False,
        "kind": "fill",
        "morning_s": 1.0,
        "plan_sha256": plan_sha,
        "recipe": RECIPE,
        "sells": [{
            "fee": 0.1,
            "fill": 11.0,
            "reason": "hold-expired",
            "shares": 5,
            "ticker": "BBB",
        }],
        "unfilled": [],
    }
    close = {
        "cost_model": "futubull+0.5%/side",
        "date": SESSION,
        "entry_date": PRIOR,
        "entry_px": 10.0,
        "fill": 11.0,
        "kind": "close",
        "pnl_primary": 1.0,
        "reason": "hold-expired",
        "recipe": RECIPE,
        "shares": 5,
        "ticker": "BBB",
    }
    return fill, close


def _ordering() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        plan = _plan()
        sealed, _line = seal(plan)
        fill, close = _fill(sealed["sha256"])
        try:
            append_records([fill, close], folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("fill before plan was accepted")
        if log_path(folder).exists() and log_path(folder).stat().st_size:
            raise SystemExit("fill before plan wrote")
        append_records([plan], folder)
        before = log_path(folder).read_bytes()
        try:
            append_records([plan], folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("duplicate plan was accepted")
        if log_path(folder).read_bytes() != before:
            raise SystemExit("duplicate plan wrote")
        bad = copy.deepcopy(plan)
        bad["date"] = "2026-09-29"
        bad["buys"] = []
        try:
            append_records([bad], folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("plan carrying buys was accepted")
        if log_path(folder).read_bytes() != before:
            raise SystemExit("bad plan wrote")
        append_records([fill, close], folder)
        filled = log_path(folder).read_bytes()
        try:
            append_records([fill, close], folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("duplicate fill was accepted")
        if log_path(folder).read_bytes() != filled:
            raise SystemExit("duplicate fill wrote")
        records = load(folder)
        if open_plan(records) is not None:
            raise SystemExit("plan still pending after fill")
        if [row["kind"] for row in records] != ["plan", "fill", "close"]:
            raise SystemExit("plan fill close order")
        later = _plan()
        later["date"] = "2026-09-29"
        later["bar_cutoff"] = SESSION
        append_records([later], folder)
        if not log_path(folder).read_bytes().startswith(filled):
            raise SystemExit("later plan rewrote the fill")


def _bar(ticker: str, day: str, op: float, close: float | None = None) -> dict:
    cl = op if close is None else close
    return {
        "close": cl,
        "date": day,
        "high": cl,
        "low": op,
        "open": op,
        "ticker": ticker,
        "volume": 1000.0,
    }


def _fetch(bars: list[dict]):
    def fetch(tickers, start, end):
        return {"bars": bars, "error": None, "missing": [], "splits": []}
    return fetch


def _pinned() -> dict:
    return {
        "AAA": {
            "close": [10.0],
            "date": [PRIOR],
            "open": [10.0],
        },
    }


def _prices() -> None:
    now = datetime(2026, 9, 28, 21, 30, tzinfo=timezone.utc)
    early = datetime(2026, 9, 28, 13, 5, tzinfo=timezone.utc)
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        first = _bar("AAA", SESSION, 10.0, 10.5)
        body = refresh(
            tickers=["AAA"],
            session=SESSION,
            held=set(),
            pinned_stored=_pinned(),
            records=[],
            fetch=_fetch([first]),
            folder=folder,
            now=early,
        )
        if body["appended"] or prices_path(folder).exists():
            raise SystemExit("pre-open refresh appended a bar")
        if "21:15" not in (body.get("why") or ""):
            raise SystemExit("pre-open refresh did not log the final-bar gate")
        body = refresh(
            tickers=["AAA"],
            session=SESSION,
            held=set(),
            pinned_stored=_pinned(),
            records=[],
            fetch=_fetch([first]),
            folder=folder,
            now=now,
        )
        if len(body["appended"]) != 1:
            raise SystemExit("post-close refresh did not append")
        stored = prices_path(folder).read_bytes()
        again = refresh(
            tickers=["AAA"],
            session=SESSION,
            held=set(),
            pinned_stored=_pinned(),
            records=[],
            fetch=_fetch([first]),
            folder=folder,
            now=now,
        )
        if again["appended"]:
            raise SystemExit("rerun appended a duplicate bar")
        if prices_path(folder).read_bytes() != stored:
            raise SystemExit("rerun rewrote a stored bar")
        if load_price_rows(folder)[0]["open"] != 10.0:
            raise SystemExit("stored open changed")
        revised = _bar("AAA", SESSION, 12.0, 10.5)
        refresh(
            tickers=["AAA"],
            session=SESSION,
            held=set(),
            pinned_stored=_pinned(),
            records=[],
            fetch=_fetch([revised]),
            folder=folder,
            now=now,
        )
        if prices_path(folder).read_bytes() != stored:
            raise SystemExit("revision overwrote a stored bar")
        if not revisions_path(folder).is_file() or b"AAA" not in revisions_path(folder).read_bytes():
            raise SystemExit("revision was not logged")
        used = [{
            "buys": [{"ticker": "AAA"}],
            "date": SESSION,
            "holdings": [],
            "kind": "fill",
            "sells": [],
        }]
        try:
            refresh(
                tickers=["AAA"],
                session=SESSION,
                held=set(),
                pinned_stored=_pinned(),
                records=used,
                fetch=_fetch([revised]),
                folder=folder,
                now=now,
            )
        except SealedBarRevision:
            pass
        else:
            raise SystemExit("sealed-bar revision did not fail")
        if prices_path(folder).read_bytes() != stored:
            raise SystemExit("sealed revision overwrote the bar")
        jump = _bar("AAA", "2026-09-29", 40.0, 40.0)
        try:
            refresh(
                tickers=["AAA"],
                session="2026-09-29",
                held={"AAA"},
                pinned_stored=_pinned(),
                records=[],
                fetch=_fetch([first, jump]),
                folder=folder,
                now=datetime(2026, 9, 29, 21, 30, tzinfo=timezone.utc),
            )
        except Halt:
            pass
        else:
            raise SystemExit("held 3x leg did not halt")
        rows = load_price_rows(folder)
        if any(row["date"] == "2026-09-29" for row in rows):
            raise SystemExit("halt appended the jumping bar")
        if prices_path(folder).read_bytes() != stored:
            raise SystemExit("halt rewrote stored bars")
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        jump = _bar("ZZZ", SESSION, 40.0)
        pinned = {"ZZZ": {"close": [10.0], "date": [PRIOR], "open": [10.0]}}
        body = refresh(
            tickers=["ZZZ"],
            session=SESSION,
            held=set(),
            pinned_stored=pinned,
            records=[],
            fetch=_fetch([jump]),
            folder=folder,
            now=now,
        )
        if not body["excluded_unexplained_legs"]:
            raise SystemExit("candidate 3x leg was not excluded")
        if not any(row["ticker"] == "ZZZ" for row in body["appended"]):
            raise SystemExit("candidate 3x bar was dropped instead of stored")
        kept = prices_path(folder).read_bytes()
        refresh(
            tickers=["ZZZ"],
            session=SESSION,
            held=set(),
            pinned_stored=pinned,
            records=[],
            fetch=_fetch([_bar("ZZZ", SESSION, 41.0)]),
            folder=folder,
            now=now,
        )
        if prices_path(folder).read_bytes() != kept:
            raise SystemExit("candidate revision overwrote the stored bar")


def _dry(out: dict) -> None:
    plan = out["plan"]
    fill = out["fill"]
    if plan["date"] != SESSION or plan["kind"] != "plan":
        raise SystemExit("dry-run plan")
    for key in ("buys", "sells", "equity_primary", "cash_primary", "holdings", "fill", "open"):
        if key in plan:
            raise SystemExit(f"plan has {key}")
    for pick in plan["picks"]:
        if "fill" in pick or "shares" in pick:
            raise SystemExit("pick has a session price")
    if fill["plan_sha256"] != plan["sha256"]:
        raise SystemExit("fill does not quote the plan")
    if fill["date"] != SESSION:
        raise SystemExit("fill date")
    picks = {row["ticker"]: row for row in plan["picks"]}
    for buy in fill["buys"]:
        if buy["ticker"] not in picks or buy["rank"] != picks[buy["ticker"]]["rank"]:
            raise SystemExit("fill buy drifted from the plan")
        if buy.get("fill_basis") != "open":
            raise SystemExit("fill basis")
    planned = {row["ticker"]: row for row in plan["planned_sells"]}
    for sell in fill["sells"]:
        row = planned.get(sell["ticker"])
        if row is None or sell["reason"] != row["reason"] or sell["shares"] != row["shares"]:
            raise SystemExit("fill sell drifted from the plan")


def _dry_append(out: dict) -> None:
    """Seal the dry-run into a temp log and refuse a second write."""
    plan = out["plan"]
    fill = out["fill"]
    closes = []
    for sell in fill["sells"]:
        match = next(row for row in out["closes"] if row["ticker"] == sell["ticker"])
        closes.append({
            "cost_model": "futubull+0.5%/side",
            "date": SESSION,
            "entry_date": PRIOR,
            "entry_px": float(sell["fill"]),
            "fill": float(sell["fill"]),
            "kind": "close",
            "pnl_primary": match["pnl_primary"],
            "reason": sell["reason"],
            "recipe": RECIPE,
            "shares": sell["shares"],
            "ticker": sell["ticker"],
        })
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        body = {key: value for key, value in plan.items() if key != "sha256"}
        append_records([body], folder)
        before = log_path(folder).read_bytes()
        try:
            append_records([body], folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("second plan write was accepted")
        if log_path(folder).read_bytes() != before:
            raise SystemExit("second plan write changed bytes")
        append_records([fill, *closes], folder)
        filled = log_path(folder).read_bytes()
        if not filled.startswith(before):
            raise SystemExit("fill rewrote the plan line")
        try:
            append_records([fill, *closes], folder)
        except RuntimeError:
            pass
        else:
            raise SystemExit("second fill write was accepted")
        if log_path(folder).read_bytes() != filled:
            raise SystemExit("second fill write changed bytes")
        records = load(folder)
        if records[0]["kind"] != "plan" or records[1]["kind"] != "fill":
            raise SystemExit("temp order")
        if records[0]["sha256"] != plan["sha256"]:
            raise SystemExit("temp plan hash")


def _sealed(tickers: tuple[str, ...]) -> list[dict]:
    return [{
        "buys": [{"ticker": ticker} for ticker in tickers],
        "date": SESSION,
        "holdings": [],
        "kind": "fill",
        "sells": [],
    }]


def _ohlc(ticker: str, op: float, high: float, low: float, close: float, volume: float = 1000.0) -> dict:
    return {
        "close": close,
        "date": SESSION,
        "high": high,
        "low": low,
        "open": op,
        "ticker": ticker,
        "volume": volume,
    }


def _sealed_tolerance() -> None:
    """Volume and one-cent reprints stay logged. One larger field is pending.

    The stored bar is never replaced. Two OHLC fields that each move by
    more than one cent still refuse the session.
    """
    now = datetime(2026, 9, 28, 21, 30, tzinfo=timezone.utc)
    fresh = _bar("FRESH", SESSION, 8.0, 8.1)

    def run(stored: list[dict], yahoo: list[dict], records: list[dict]) -> dict:
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            seeded = refresh(
                tickers=sorted({row["ticker"] for row in stored}),
                session=SESSION,
                held=set(),
                pinned_stored=_pinned(),
                records=[],
                fetch=_fetch(stored),
                folder=folder,
                now=now,
            )
            if len(seeded["appended"]) != len(stored):
                raise SystemExit(f"seed failed {seeded}")
            kept = [dict(row) for row in load_price_rows(folder)]
            raised = None
            body = None
            try:
                body = refresh(
                    tickers=sorted({row["ticker"] for row in yahoo + [fresh]}),
                    session=SESSION,
                    held=set(),
                    pinned_stored=_pinned(),
                    records=records,
                    fetch=_fetch(yahoo + [fresh]),
                    folder=folder,
                    now=now,
                )
            except SealedBarRevision as exc:
                raised = str(exc)
            return {
                "body": body,
                "kept": kept,
                "raised": raised,
                "rows": [dict(row) for row in load_price_rows(folder)],
            }

    volume = run(
        [_ohlc("FEAM", 4.0, 4.2, 3.9, 4.1, 1000)],
        [_ohlc("FEAM", 4.0, 4.2, 3.9, 4.1, 5000)],
        _sealed(("FEAM",)),
    )
    if volume["raised"]:
        raise SystemExit(f"volume-only sealed revision refused {volume['raised']}")
    if volume["rows"][0]["volume"] != 1000.0:
        raise SystemExit("volume-only overwrote the stored bar")
    if not any(row["ticker"] == "FRESH" for row in volume["rows"]):
        raise SystemExit("volume-only did not append the new bar")

    cent = run(
        [_ohlc("GLND", 5.138, 5.2, 5.0, 5.2)],
        [_ohlc("GLND", 5.14, 5.2, 5.0, 5.2)],
        _sealed(("GLND",)),
    )
    if cent["raised"] or (cent["body"] or {}).get("pending"):
        raise SystemExit(f"one-cent sealed revision refused {cent}")
    if cent["rows"][0]["open"] != 5.138:
        raise SystemExit("one-cent revision overwrote the stored open")

    pending = run(
        [_ohlc("USDE", 16.85, 17.065001, 16.4, 16.9)],
        [_ohlc("USDE", 16.559999, 17.07, 16.4, 16.9)],
        _sealed(("USDE",)),
    )
    if pending["raised"]:
        raise SystemExit(f"single pending field refused {pending['raised']}")
    if pending["rows"][0]["open"] != 16.85:
        raise SystemExit("pending open was stored")
    got = (pending["body"] or {}).get("pending")
    expect = {"field": "open", "new": 16.559999, "old": 16.85, "ticker": "USDE"}
    if got != expect:
        raise SystemExit(f"pending leg {got}")
    if not any(row["ticker"] == "FRESH" for row in pending["rows"]):
        raise SystemExit("pending field did not append the new bar")


def _float32_overlay() -> None:
    """The jsonl bar is the stored print, including over a different parquet open."""
    import numpy as np

    exact_open = 4.68
    exact_close = 4.375
    noisy_open = float(np.float32(exact_open))
    noisy_close = float(np.float32(exact_close))
    if noisy_open == exact_open:
        raise SystemExit("fixture open is not a float32 image")
    row = {
        "close": exact_close,
        "date": SESSION,
        "high": 5.07,
        "low": 4.36,
        "open": exact_open,
        "ticker": "SHMD",
        "volume": 5322584.0,
    }
    bars = {
        "feat": {
            "SHMD": {
                "adjusted": True,
                "close": np.array([noisy_close]),
                "date": [SESSION],
                "high": np.array([5.07]),
                "low": np.array([4.36]),
                "open": np.array([noisy_open]),
                "volume": np.array([999.0]),
            }
        },
        "stored": {
            "SHMD": {
                "close": np.array([noisy_close]),
                "date": [SESSION],
                "open": np.array([noisy_open]),
            }
        },
    }
    out = overlay_rows(bars, [row])
    if float(out["stored"]["SHMD"]["open"][0]) != exact_open:
        raise SystemExit("overlay kept the float32 open")
    if float(out["stored"]["SHMD"]["close"][0]) != exact_close:
        raise SystemExit("overlay kept the float32 close")
    if float(out["feat"]["SHMD"]["volume"][0]) != 5322584.0:
        raise SystemExit("overlay left the parquet volume on a stored bar")
    moved = {
        "feat": {
            "SHMD": {
                "adjusted": True,
                "close": np.array([noisy_close]),
                "date": [SESSION],
                "high": np.array([5.07]),
                "low": np.array([4.36]),
                "open": np.array([16.56]),
                "volume": np.array([999.0]),
            }
        },
        "stored": {
            "SHMD": {
                "close": np.array([noisy_close]),
                "date": [SESSION],
                "open": np.array([16.56]),
            }
        },
    }
    other = dict(row)
    other["open"] = 16.85
    kept = overlay_rows(moved, [other])
    if float(kept["stored"]["SHMD"]["open"][0]) != 16.85:
        raise SystemExit("overlay kept a parquet open over the stored jsonl print")


def main() -> None:
    _ordering()
    _prices()
    _sealed_tolerance()
    _float32_overlay()
    log_before = (ROOT / "research/hot_n4_clean_v4/forward/holdup_log.jsonl").read_bytes()
    led_before = (ROOT / "research/hot_n4_clean_v4/forward/LEDGER.jsonl").read_bytes()
    out = simulate()
    if (ROOT / "research/hot_n4_clean_v4/forward/holdup_log.jsonl").read_bytes() != log_before:
        raise SystemExit("dry-run changed the sealed log")
    if (ROOT / "research/hot_n4_clean_v4/forward/LEDGER.jsonl").read_bytes() != led_before:
        raise SystemExit("dry-run changed the sealed ledger")
    _dry(out)
    _dry_append(out)
    print(
        "plan-fill ok",
        out["session"],
        "picks", len(out["picks"]),
        "sells", len(out["planned_sells"]),
        "buys", len(out["fill"]["buys"]),
        "equity", round(out["equity_primary"], 2),
    )


if __name__ == "__main__":
    main()
