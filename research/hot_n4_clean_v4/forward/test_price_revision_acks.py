"""Exact-match acks for a sealed Yahoo bar revision.

An approved ack line removes only that one field revision from the stop
count. The stored bar is not overwritten, the revision is still logged,
a revision with no ack (or a PENDING ack, or another Yahoo value) still
stops the run, and a malformed ack file refuses.
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.prices import (  # noqa: E402
    ACKS_NAME,
    SealedBarRevision,
    ack_is_approved,
    acks_path,
    load_acks,
    prices_path,
    refresh,
    revisions_path,
)

H1 = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1"
NOW = datetime(2026, 10, 7, 21, 30, tzinfo=timezone.utc)
SESSION = "2026-10-01"


def _dumps(row: dict) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def _bar(ticker, day, op, high, low, close, volume=1000.0):
    return {"close": close, "date": day, "high": high, "low": low, "open": op,
            "ticker": ticker, "volume": volume}


def _fetch(bars):
    def fetch(tickers, start, end):
        return {"bars": bars, "error": None, "missing": [], "splits": []}
    return fetch


def _ack(ticker, day, field, old, new, approved_by="Test 2026-10-07"):
    return {"approved_by": approved_by, "date": day, "field": field,
            "kind": "price_revision_ack", "new": new, "old": old,
            "reason": "yahoo_data_revision_not_split",
            "sealed_record_sha256": "0" * 64, "ticker": ticker}


def _used(day, tickers):
    return [{"buys": [{"ticker": t} for t in tickers], "date": day,
             "holdings": [], "kind": "fill", "sells": []}]


def _run(folder, bars, records, session=SESSION):
    return refresh(tickers=sorted({b["ticker"] for b in bars}), session=session,
                   held=set(), pinned_stored={}, records=records,
                   fetch=_fetch(bars), folder=folder, now=NOW)


def _expect_stop(folder, bars, records, label, session=SESSION):
    stored = prices_path(folder).read_bytes()
    try:
        _run(folder, bars, records, session)
    except SealedBarRevision:
        pass
    else:
        raise SystemExit(f"{label}: run did not stop")
    if prices_path(folder).read_bytes() != stored:
        raise SystemExit(f"{label}: stored bars changed")


def _synthetic() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        base = [_bar("AAA", SESSION, 10.0, 10.0, 9.0, 9.5),
                _bar("BBB", SESSION, 5.0, 5.5, 4.5, 5.2)]
        prices_path(folder).write_text("".join(_dumps(b) + "\n" for b in base))
        stored = prices_path(folder).read_bytes()
        revised = [_bar("AAA", SESSION, 10.5, 10.5, 9.0, 9.5),
                   _bar("BBB", SESSION, 5.1, 5.5, 4.5, 5.2)]
        used = _used(SESSION, ["AAA", "BBB"])
        # No ack: three sealed fields moved, the run stops.
        _expect_stop(folder, revised, used, "no ack")
        logged = revisions_path(folder).read_text().count("\n")
        if logged != 2:
            raise SystemExit("revisions were not logged without an ack")
        # PENDING acks are not honored.
        acks = [_ack("AAA", SESSION, "open", 10.0, 10.5, "PENDING Cyrus"),
                _ack("AAA", SESSION, "high", 10.0, 10.5, "PENDING Cyrus"),
                _ack("BBB", SESSION, "open", 5.0, 5.1, "PENDING Cyrus")]
        acks_path(folder).write_text("".join(_dumps(a) + "\n" for a in acks))
        if any(ack_is_approved(a) for a in load_acks(folder)):
            raise SystemExit("PENDING ack counted as approved")
        _expect_stop(folder, revised, used, "pending acks")
        # Approved acks for AAA only: BBB stays one pending leg, run continues.
        acks = [_ack("AAA", SESSION, "open", 10.0, 10.5),
                _ack("AAA", SESSION, "high", 10.0, 10.5)]
        acks_path(folder).write_text("".join(_dumps(a) + "\n" for a in acks))
        body = _run(folder, revised, used)
        if (body.get("pending") or {}).get("ticker") != "BBB":
            raise SystemExit("unacked BBB leg was not left pending")
        if len(body.get("acknowledged") or []) != 2:
            raise SystemExit("acknowledged legs not listed in the ledger")
        # A third revision with no ack plus the BBB leg: two material, stop.
        third = revised + [_bar("CCC", SESSION, 1.0, 1.2, 0.9, 1.1)]
        prices_path(folder).write_text(
            prices_path(folder).read_text()
            + _dumps(_bar("CCC", SESSION, 1.05, 1.2, 0.9, 1.1)) + "\n"
        )
        _expect_stop(folder, third, _used(SESSION, ["AAA", "BBB", "CCC"]), "unacked pair")
        prices_path(folder).write_bytes(stored)
        # Every field acked: no pending, nothing overwritten, still logged.
        acks.append(_ack("BBB", SESSION, "open", 5.0, 5.1))
        acks_path(folder).write_text("".join(_dumps(a) + "\n" for a in acks))
        before = revisions_path(folder).read_text().count("\n")
        body = _run(folder, revised, used)
        if body.get("pending") or len(body.get("acknowledged") or []) != 3:
            raise SystemExit("fully acked revision did not pass cleanly")
        if prices_path(folder).read_bytes() != stored:
            raise SystemExit("acked revision overwrote a stored bar")
        if revisions_path(folder).read_text().count("\n") != before + 2:
            raise SystemExit("acked revision was not logged")
        # A different Yahoo value for an acked field is not a match.
        moved = [_bar("AAA", SESSION, 10.6, 10.6, 9.0, 9.5),
                 _bar("BBB", SESSION, 5.1, 5.5, 4.5, 5.2)]
        _expect_stop(folder, moved, used, "acked field moved again")
        # A malformed ack line refuses.
        acks_path(folder).write_text('{"kind":"price_revision_ack"}\n')
        try:
            _run(folder, revised, used)
        except RuntimeError as exc:
            if isinstance(exc, SealedBarRevision):
                raise SystemExit("malformed ack was read as a revision stop")
        else:
            raise SystemExit("malformed ack file did not refuse")
        if prices_path(folder).read_bytes() != stored:
            raise SystemExit("malformed ack run changed stored bars")


def _h1_replay() -> None:
    """Replay run 37674833077's Yahoo prints against the real h1 store."""
    lines = [json.loads(line) for line in (H1 / "price_revisions.jsonl").read_text().splitlines()]
    bars = [row["new"] for row in lines if row["at"] == "2026-10-07T19:32:00Z"]
    if len(bars) != 24:
        raise SystemExit(f"expected 24 revisions from run 37674833077, found {len(bars)}")
    records = [json.loads(line) for line in (H1 / "h1_log.jsonl").read_text().splitlines()]
    real_acks = load_acks(H1)
    if len(real_acks) != 3:
        raise SystemExit("forward_h1 ack file should hold three lines")
    with tempfile.TemporaryDirectory() as tmp:
        folder = Path(tmp)
        shutil.copyfile(H1 / "prices.jsonl", prices_path(folder))
        stored = prices_path(folder).read_bytes()
        _expect_stop(folder, bars, records, "h1 no acks", session="2026-10-07")
        shutil.copyfile(H1 / ACKS_NAME, acks_path(folder))
        if all(ack_is_approved(a) for a in real_acks):
            body = _run(folder, bars, records, session="2026-10-07")
            if body.get("pending") or len(body.get("acknowledged") or []) != 3:
                raise SystemExit("approved h1 acks did not clear run 37674833077")
        else:
            _expect_stop(folder, bars, records, "h1 pending acks", session="2026-10-07")
        approved = [dict(a, approved_by="Test approval") for a in real_acks]
        acks_path(folder).write_text("".join(_dumps(a) + "\n" for a in approved))
        body = _run(folder, bars, records, session="2026-10-07")
        if body.get("pending") or len(body.get("acknowledged") or []) != 3:
            raise SystemExit("approved h1 acks did not clear run 37674833077")
        acks_path(folder).write_text("".join(_dumps(a) + "\n" for a in approved[:2]))
        body = _run(folder, bars, records, session="2026-10-07")
        if (body.get("pending") or {}).get("ticker") != "PACB":
            raise SystemExit("KOD-only acks did not leave PACB pending")
        if prices_path(folder).read_bytes() != stored:
            raise SystemExit("h1 replay changed stored bars")


def main() -> None:
    _synthetic()
    _h1_replay()
    print("price revision acks ok")


if __name__ == "__main__":
    main()
