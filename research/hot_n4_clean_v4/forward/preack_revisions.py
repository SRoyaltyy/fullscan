"""Pre-seal Yahoo revision acks (add-only), run ~07:30 ET before the h1 seal.

Cyrus gave standing approval (2026-10-20 / 2026-10-21 08:29 HKT) for
add-only price-revision acks that keep fullscan running. Twice in three
days a Yahoo revision on a sealed bar blocked the h1 seal at ~09:05 ET.

This script asks Yahoo the same question ``prices.refresh`` asks and
uses the same detection code (``sealed_bar_keys``, ``_same``,
``_material_ohlc`` with its >1 cent rule, ``approved_ack_keys``,
``split_acknowledged``). Every sealed-bar OHLC move the seal would count
and that is not acked yet gets one ack line appended to
``price_revision_acks.jsonl`` of that book, in the shape of commits
c714d187 / d2d1449dc.

It writes ONLY the acks file. It never touches the sealed log, the
ledger, ``prices.jsonl``, ``price_revisions.jsonl`` or
``PRICE_LEDGER.jsonl``. A ticker with a Yahoo split/dividend action, or a
move over 5%, is not acked (that is not a plain data revision) and is
printed so a person looks at it.

``--dry-run`` prints the lines and writes nothing.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.book import BOOKS, reset_book, use_book  # noqa: E402
from research.hot_n4_clean_v4.forward.ledger import canonical_bytes, load  # noqa: E402
from research.hot_n4_clean_v4.forward.prices import (  # noqa: E402
    ACK_KIND,
    PIN_END,
    _append_lines,
    _material_ohlc,
    _round_bar,
    _same,
    acks_path,
    approved_ack_keys,
    bar_is_final,
    fetch_yahoo,
    load_acks,
    load_price_rows,
    sealed_bar_keys,
    split_acknowledged,
)

APPROVAL = (
    "Standing approval (Cyrus 2026-10-20 / 2026-10-21 08:29 HKT): "
    "price-revision acks needed to keep fullscan running, no yes/no card"
)
EVIDENCE = (
    "Yahoo split-adjusted daily bar (auto_adjust=False, actions=True): "
    "no split or dividend event; pre-seal check found {field} moved over 1c"
)
MAX_REL = 0.05


def _records_using(records: list[dict], ticker: str, session: str) -> list[dict]:
    out = []
    for rec in records:
        if (ticker, session) in sealed_bar_keys([rec]) and rec.get("sha256"):
            out.append(rec)
    return out


def ack_lines(book, records, stored_rows, fetched, now, run_id) -> tuple[list[dict], list[str]]:
    """(new ack rows, skipped notes) for one book. Pure: writes nothing."""
    known = {(r["ticker"], r["date"]): r for r in stored_rows}
    used = sealed_bar_keys(records)
    approved = approved_ack_keys(load_acks(book.folder))
    actions = {str(s.get("ticker", "")).upper() for s in fetched.get("splits") or []}
    et = now.astimezone(ZoneInfo("America/New_York"))
    rows, skipped, seen = [], [], set()
    for raw in fetched.get("bars") or []:
        new = _round_bar(raw)
        if new["date"] <= PIN_END or not bar_is_final(new["date"], now):
            continue
        old = known.get((new["ticker"], new["date"]))
        if old is None or _same(old, new) or (new["ticker"], new["date"]) not in used:
            continue
        legs, _ = split_acknowledged(_material_ohlc(old, new), new["date"], approved)
        for leg in legs:
            key = (leg["ticker"], new["date"], leg["field"])
            if key in seen:
                continue
            seen.add(key)
            rel = abs(leg["new"] - leg["old"]) / max(abs(leg["old"]), 1e-9)
            if leg["ticker"] in actions or rel > MAX_REL:
                skipped.append(f"{book.folder_name} {key} old {leg['old']} new {leg['new']}: "
                               "split/action or >5% move, left for a person")
                continue
            recs = _records_using(records, leg["ticker"], new["date"])
            kinds = sorted({str(r.get("kind")) for r in recs})
            rows.append({
                "approval_source": APPROVAL,
                "approved_at": et.replace(second=0, microsecond=0).isoformat(),
                "approved_by": "Cyrus",
                "book": "h1" if book.folder_name == "forward_h1" else "holdup",
                "date": new["date"],
                "evidence": EVIDENCE.format(field=leg["field"]),
                "field": leg["field"],
                "first_seen": now.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "kind": ACK_KIND,
                "new": leg["new"],
                "old": leg["old"],
                "reason": "yahoo_data_revision_not_split",
                "run": str(run_id),
                "sealed_record_sha256": recs[0]["sha256"] if recs else "",
                "sealed_records": [r["sha256"] for r in recs],
                "sealed_use": (f"{new['date']} {leg['ticker']} in sealed {'/'.join(kinds)} records; "
                               "pre-seal ack (stored bar kept; no sealed record rewritten)"),
                "ticker": leg["ticker"],
            })
    return rows, skipped


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    # Same in-memory notes the h1 seal runs under (h1 only; holdup untouched).
    from research.hot_n4_clean_v4.forward import h1_fill_missing
    h1_fill_missing.install()
    now = datetime.now(timezone.utc)
    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    end = (now.date() + timedelta(days=1)).isoformat()
    total = 0
    for book in BOOKS:
        token = use_book(book)
        try:
            records = load(book.folder)
            stored = load_price_rows(book.folder)
            used = sealed_bar_keys(records)
            tickers = sorted({t for (t, d) in used if d > PIN_END and (t, d) in
                              {(r["ticker"], r["date"]) for r in stored}})
            if not tickers:
                print(f"{book.folder_name}: no sealed forward bars")
                continue
            start = min(r["date"] for r in stored)
            fetched = fetch_yahoo(tickers, start, end)
            if fetched.get("error"):
                print(f"{book.folder_name}: Yahoo error {fetched['error']}; no acks")
                continue
            rows, skipped = ack_lines(book, records, stored, fetched, now, run_id)
            for note in skipped:
                print("SKIP " + note)
            for row in rows:
                print(f"{'WOULD ACK' if args.dry_run else 'ACK'} {book.folder_name} "
                      f"{row['ticker']} {row['date']} {row['field']} {row['old']} -> {row['new']}")
            print(f"{book.folder_name}: {len(tickers)} tickers checked, {len(rows)} new acks")
            if rows and not args.dry_run:
                _append_lines(acks_path(book.folder), [canonical_bytes(r) for r in rows])
                load_acks(book.folder)  # same parser the seal uses must accept it
            total += len(rows)
        finally:
            reset_book(token)
    print(f"total new acks: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
