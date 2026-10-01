"""Plan-before-fill, idempotent reruns, sealed bars, and the 2026-09-28 dry-run."""
from __future__ import annotations

import copy
import json
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
    price_ledger_path,
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
        # Open and low both move by more than one cent (12 vs 10). That is
        # not the single pending leg, so the session still refuses.
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


def _quote(
    ticker: str,
    day: str,
    op: float,
    high: float,
    low: float,
    close: float,
    volume: float = 1000.0,
) -> dict:
    return {
        "close": close,
        "date": day,
        "high": high,
        "low": low,
        "open": op,
        "ticker": ticker,
        "volume": float(volume),
    }


def _sealed(*tickers: str, day: str = SESSION) -> list[dict]:
    return [{
        "buys": [{"ticker": ticker} for ticker in tickers],
        "date": day,
        "holdings": [],
        "kind": "fill",
        "sells": [],
    }]


def _guard(
    stored: list[dict],
    yahoo: list[dict],
    *,
    records: list[dict],
    extra: list[dict] | None = None,
    missing: list[dict] | None = None,
) -> dict:
    """Seed ``stored``, then refresh ``yahoo`` plus any fresh bars. Temp dir only."""
    extra = list(extra or [])
    missing = list(missing or [])
    now = datetime(2026, 9, 28, 21, 30, tzinfo=timezone.utc)
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        names = sorted({
            row["ticker"]
            for row in list(stored) + list(yahoo) + extra + missing
        })
        if stored:
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
                raise SystemExit(f"seed did not store {[row['ticker'] for row in stored]} {seeded}")
        kept = [dict(row) for row in load_price_rows(folder)]

        def fetch(tickers, start, end):
            return {
                "bars": list(yahoo) + extra,
                "error": None,
                "missing": missing,
                "splits": [],
            }

        raised = None
        body = None
        try:
            body = refresh(
                tickers=names,
                session=SESSION,
                held=set(),
                pinned_stored=_pinned(),
                records=records,
                fetch=fetch,
                folder=folder,
                now=now,
            )
        except SealedBarRevision as exc:
            raised = str(exc)
        rev_rows = []
        rev_path = revisions_path(folder)
        if rev_path.is_file():
            rev_rows = [json.loads(line) for line in rev_path.read_text().splitlines() if line]
        ledger = []
        led = price_ledger_path(folder)
        if led.is_file():
            ledger = [json.loads(line) for line in led.read_text().splitlines() if line]
        return {
            "body": body,
            "kept": kept,
            "ledger": ledger,
            "raised": raised,
            "revisions": rev_rows,
            "rows": [dict(row) for row in load_price_rows(folder)],
        }


def _row(rows: list[dict], ticker: str) -> dict:
    found = [row for row in rows if row["ticker"] == ticker]
    if len(found) != 1:
        raise SystemExit(f"{ticker} rows {found}")
    return found[0]


def _unchanged(out: dict, ticker: str) -> None:
    old = _row(out["kept"], ticker)
    new = _row(out["rows"], ticker)
    if new != old:
        raise SystemExit(f"{ticker} stored bar changed {old} -> {new}")


def _fresh_appended(out: dict, ticker: str) -> None:
    if out["body"] is None:
        raise SystemExit(f"{ticker} fresh bar had no ledger body")
    if not any(row["ticker"] == ticker for row in out["body"]["appended"]):
        raise SystemExit(f"{ticker} fresh bar was not appended")
    _row(out["rows"], ticker)


def _sealed_volume_only() -> None:
    stored = [
        _quote("FEAM", SESSION, 4.0, 4.2, 3.9, 4.1, 1000),
        _quote("SHMD", SESSION, 5.0, 5.2, 4.9, 5.1, 2000),
        _quote("SRFM", SESSION, 6.0, 6.2, 5.9, 6.1, 3000),
        _quote("TJGC", SESSION, 7.0, 7.2, 6.9, 7.1, 4000),
    ]
    yahoo = [
        _quote("FEAM", SESSION, 4.0, 4.2, 3.9, 4.1, 1100),
        _quote("SHMD", SESSION, 5.0, 5.2, 4.9, 5.1, 2100),
        _quote("SRFM", SESSION, 6.0, 6.2, 5.9, 6.1, 3100),
        _quote("TJGC", SESSION, 7.0, 7.2, 6.9, 7.1, 4100),
    ]
    out = _guard(
        stored,
        yahoo,
        records=_sealed("FEAM", "SHMD", "SRFM", "TJGC"),
        extra=[_quote("FRESH", SESSION, 8.0, 8.1, 7.9, 8.0, 500)],
    )
    if out["raised"]:
        raise SystemExit(f"volume-only sealed revision refused the session {out['raised']}")
    for ticker in ("FEAM", "SHMD", "SRFM", "TJGC"):
        _unchanged(out, ticker)
    _fresh_appended(out, "FRESH")
    logged = {row["ticker"] for row in out["revisions"] if row.get("sealed_use")}
    if logged != {"FEAM", "SHMD", "SRFM", "TJGC"}:
        raise SystemExit(f"volume-only revisions {logged}")
    if out["body"].get("pending"):
        raise SystemExit("volume-only left a pending leg")


def _sealed_glnd_cent() -> None:
    stored = [_quote("GLND", SESSION, 5.138, 5.705, 5.0, 5.2, 1000)]
    yahoo = [_quote("GLND", SESSION, 5.14, 5.71, 5.0, 5.2, 1800)]
    out = _guard(
        stored,
        yahoo,
        records=_sealed("GLND"),
        extra=[_quote("FRESH", SESSION, 8.0, 8.1, 7.9, 8.0, 500)],
    )
    if out["raised"]:
        raise SystemExit(f"GLND cent rounding refused the session {out['raised']}")
    _unchanged(out, "GLND")
    _fresh_appended(out, "FRESH")
    if not any(row["ticker"] == "GLND" and row.get("sealed_use") for row in out["revisions"]):
        raise SystemExit("GLND cent revision was not logged")
    if out["body"].get("pending"):
        raise SystemExit("GLND cent rounding left a pending leg")


def _sealed_secz_cent() -> None:
    stored = [_quote("SECZ", SESSION, 16.5, 17.00, 16.2, 16.8, 1000)]
    yahoo = [_quote("SECZ", SESSION, 16.5, 17.01, 16.2, 16.8, 1800)]
    out = _guard(
        stored,
        yahoo,
        records=_sealed("SECZ"),
        extra=[_quote("FRESH", SESSION, 8.0, 8.1, 7.9, 8.0, 500)],
    )
    if out["raised"]:
        raise SystemExit(f"SECZ high 17.00 vs 17.01 refused the session {out['raised']}")
    _unchanged(out, "SECZ")
    if _row(out["rows"], "SECZ")["high"] != 17.0:
        raise SystemExit("SECZ stored high was overwritten")
    _fresh_appended(out, "FRESH")
    if not any(row["ticker"] == "SECZ" and row.get("sealed_use") for row in out["revisions"]):
        raise SystemExit("SECZ cent revision was not logged")
    if out["body"].get("pending"):
        raise SystemExit("SECZ one-cent high left a pending leg")


def _sealed_usde_pending() -> None:
    stored = [_quote("USDE", SESSION, 16.85, 17.065001, 16.4, 16.9, 1000)]
    yahoo = [_quote("USDE", SESSION, 16.559999, 17.07, 16.4, 16.9, 2500)]
    out = _guard(
        stored,
        yahoo,
        records=_sealed("USDE"),
        extra=[_quote("FRESH", SESSION, 8.0, 8.1, 7.9, 8.0, 500)],
    )
    if out["raised"]:
        raise SystemExit(f"USDE pending open refused the session {out['raised']}")
    _unchanged(out, "USDE")
    if _row(out["rows"], "USDE")["open"] != 16.85:
        raise SystemExit("USDE stored open was overwritten")
    if any(row["open"] == 16.559999 for row in out["rows"]):
        raise SystemExit("USDE pending open was stored")
    _fresh_appended(out, "FRESH")
    pending = out["body"].get("pending")
    expect = {"field": "open", "new": 16.559999, "old": 16.85, "ticker": "USDE"}
    if pending != expect:
        raise SystemExit(f"USDE pending leg {pending}")
    why = "pending USDE open old 16.85 new 16.559999"
    if out["body"].get("why") != why:
        raise SystemExit(f"USDE ledger why {out['body'].get('why')}")
    if not out["ledger"] or out["ledger"][-1].get("pending") != expect:
        raise SystemExit(f"USDE ledger line {out['ledger'][-1] if out['ledger'] else None}")
    if out["ledger"][-1].get("why") != why:
        raise SystemExit("USDE ledger line did not name the pending leg")


def _sealed_hyac_missing() -> None:
    fresh = _quote("FRESH", SESSION, 8.0, 8.1, 7.9, 8.0, 500)
    missing = [{"reason": "no bars", "ticker": "HYAC-U"}]
    out = _guard([], [], records=[], extra=[fresh], missing=missing)
    if out["raised"]:
        raise SystemExit(f"HYAC-U missing refused the session {out['raised']}")
    if out["body"]["missing"] != missing:
        raise SystemExit(f"HYAC-U missing {out['body']['missing']}")
    _fresh_appended(out, "FRESH")
    if any(row["ticker"] == "HYAC-U" for row in out["rows"]):
        raise SystemExit("HYAC-U missing bar was stored")


def _sealed_two_fields_still_refuse() -> None:
    stored = [_quote("AAA", SESSION, 10.0, 11.0, 9.0, 10.5, 1000)]
    yahoo = [_quote("AAA", SESSION, 12.0, 11.0, 9.0, 13.0, 1000)]
    out = _guard(
        stored,
        yahoo,
        records=_sealed("AAA"),
        extra=[_quote("FRESH", SESSION, 8.0, 8.1, 7.9, 8.0, 500)],
    )
    if out["raised"] != "Yahoo revised a bar a sealed record used":
        raise SystemExit(f"two-field sealed move did not refuse {out['raised']}")
    _unchanged(out, "AAA")
    if any(row["ticker"] == "FRESH" for row in out["rows"]):
        raise SystemExit("two-field sealed move appended another ticker")
    if out["body"] is not None:
        raise SystemExit("two-field sealed move returned a body")


def _sealed_guard() -> None:
    _sealed_volume_only()
    _sealed_glnd_cent()
    _sealed_secz_cent()
    _sealed_usde_pending()
    _sealed_hyac_missing()
    _sealed_two_fields_still_refuse()
    print("sealed-bar guard ok")


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


def main() -> None:
    _ordering()
    _prices()
    _sealed_guard()
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
