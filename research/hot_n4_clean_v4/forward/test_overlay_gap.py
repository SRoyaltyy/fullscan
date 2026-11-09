"""overlay_rows: a post-pin jsonl bar in a gap of the parquet is placed in order.

2026-11-09: data/prices/ohlc.parquet (01e8260507) held XP 2026-10-12 then
2026-10-16, while prices.jsonl stored 10-13..10-15. The overlay raised
'forward bar out of order XP 2026-10-13' and the holdup run skipped.
"""
import sys
import unittest
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

from research.hot_n4_clean_v4.forward.prices import PIN_END, overlay_rows  # noqa: E402


def blob(dates, feat=True):
    n = len(dates)
    out = {"date": list(dates), "adjusted": True,
           "open": np.arange(n, dtype=float) + 10.0,
           "close": np.arange(n, dtype=float) + 20.0}
    if feat:
        out["high"] = np.arange(n, dtype=float) + 30.0
        out["low"] = np.arange(n, dtype=float) + 5.0
        out["volume"] = np.arange(n, dtype=float) + 100.0
    return out


def row(day, px):
    return {"ticker": "XP", "date": day, "open": px, "high": px + 1, "low": px - 1,
            "close": px + 0.5, "volume": 7.0}


class OverlayGap(unittest.TestCase):
    def setUp(self):
        days = [PIN_END, "2026-10-12", "2026-10-16"]
        self.bars = {"stored": {"XP": blob(days, feat=False)}, "feat": {"XP": blob(days)}}

    def test_gap_bars_are_inserted_in_order(self):
        rows = [row("2026-10-13", 1.0), row("2026-10-14", 2.0), row("2026-10-16", 3.0),
                row("2026-10-19", 4.0)]
        out = overlay_rows(self.bars, rows)
        want = [PIN_END, "2026-10-12", "2026-10-13", "2026-10-14", "2026-10-16", "2026-10-19"]
        for side, fields in (("stored", ("open", "close")),
                             ("feat", ("open", "high", "low", "close", "volume"))):
            got = out[side]["XP"]
            self.assertEqual(got["date"], want)
            for field in fields:
                self.assertEqual(len(got[field]), len(want), (side, field))
        self.assertEqual(list(out["stored"]["XP"]["open"]), [10.0, 11.0, 1.0, 2.0, 3.0, 4.0])
        self.assertEqual(list(out["feat"]["XP"]["volume"]), [100.0, 101.0, 7.0, 7.0, 7.0, 7.0])
        # The input is not mutated.
        self.assertEqual(self.bars["stored"]["XP"]["date"], [PIN_END, "2026-10-12", "2026-10-16"])

    def test_pinned_date_still_refuses(self):
        with self.assertRaises(RuntimeError):
            overlay_rows(self.bars, [row(PIN_END, 1.0)])

    def test_unfilled_array_still_refuses(self):
        self.bars["stored"]["XP"]["high"] = np.array([1.0, 2.0, 3.0])
        with self.assertRaises(RuntimeError):
            overlay_rows(self.bars, [row("2026-10-13", 1.0)])


if __name__ == "__main__":
    unittest.main()
