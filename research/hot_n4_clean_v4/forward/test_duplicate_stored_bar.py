"""Read-only dedupe of a repeated (ticker, session) line in prices.jsonl.

2026-11-12: h1 open fill runs 43307104372 and 43307742129 each appended the
11-12 bars (the second checked out the event SHA from before the first
pushed). The add-only union rebase kept the two lines whose open differed
(GEVO open 1.81 vs 1.82, HUMA open 0.5872 vs 0.5844). Every later h1 run
refused on ``duplicate stored bar``.

An exact repeat is dropped from the read. A repeat whose values differ
still refuses unless an approved ``duplicate_bar_acks.jsonl`` line names
both rows exactly; then the first stored line is the stored bar. The file
is never rewritten.
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.prices import (  # noqa: E402
    DUP_ACK_KIND,
    duplicate_acks_path,
    load_price_rows,
    prices_path,
)

H1 = ROOT / "research" / "hot_n4_clean_v4" / "forward_h1"


def _dumps(row):
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def _bar(ticker, op, day="2026-11-12"):
    return {"close": 1.86, "date": day, "high": 1.965, "low": 1.75, "open": op,
            "ticker": ticker, "volume": 12077450.0}


def _write(folder, rows, acks=None):
    prices_path(folder).write_text("".join(_dumps(r) + "\n" for r in rows))
    if acks is not None:
        duplicate_acks_path(folder).write_text("".join(_dumps(a) + "\n" for a in acks))


def _ack(kept, dropped, approved_by="Test 2026-11-12"):
    return {"approved_by": approved_by, "date": kept["date"], "dropped": dropped,
            "kept": kept, "kind": DUP_ACK_KIND, "reason": "test", "ticker": kept["ticker"]}


def _refuses(folder):
    try:
        load_price_rows(folder)
    except RuntimeError as exc:
        return "duplicate stored bar" in str(exc)
    return False


def main() -> int:
    tmp = Path(tempfile.mkdtemp())
    try:
        first, second = _bar("GEVO", 1.81), _bar("GEVO", 1.82)
        other = _bar("IWM", 278.44)

        _write(tmp, [first, other, dict(first)])
        rows = load_price_rows(tmp)
        assert len(rows) == 2 and rows[0]["open"] == 1.81, rows

        _write(tmp, [first, other, second])
        assert _refuses(tmp), "different repeat without ack must refuse"

        _write(tmp, [first, other, second], [_ack(first, second, "PENDING Cyrus")])
        assert _refuses(tmp), "PENDING ack must not be honored"

        _write(tmp, [first, other, second], [_ack(second, first)])
        assert _refuses(tmp), "ack with kept/dropped swapped must not match"

        _write(tmp, [first, other, second], [_ack(first, _bar("GEVO", 1.83))])
        assert _refuses(tmp), "ack naming another print must not match"

        before = prices_path(tmp).read_bytes()
        _write(tmp, [first, other, second], [_ack(first, second)])
        rows = load_price_rows(tmp)
        gevo = [r for r in rows if r["ticker"] == "GEVO"]
        assert len(gevo) == 1 and gevo[0]["open"] == 1.81, gevo
        assert prices_path(tmp).read_bytes() == before, "prices.jsonl must not change"

        bad = _ack(first, second)
        bad["dropped"] = _bar("HUMA", 0.5844)
        _write(tmp, [first, other, second], [bad])
        try:
            load_price_rows(tmp)
            raise AssertionError("ack whose row is another ticker must refuse")
        except RuntimeError as exc:
            assert "duplicate_bar_acks.jsonl" in str(exc), exc

        # The real h1 store loads with the committed acks.
        if prices_path(H1).is_file():
            load_price_rows(H1)
    finally:
        shutil.rmtree(tmp)
    print("duplicate stored bar: OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
