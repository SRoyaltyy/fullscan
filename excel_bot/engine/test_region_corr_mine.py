"""Unit tests for region_corr_mine — clock + deep-green intuition."""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from region_corr_mine import (  # noqa: E402
    _fwd,
    _hi_from_ohlc,
    _hold_to_region_end,
    _streaks,
    features_at_open,
    iter_rows,
    mine_days_map,
)


def _day(date, o, h, l, c, v=1_000_000):
    return {"date": date, "open": o, "high": h, "low": l, "close": c, "volume": v}


class RegionMath(unittest.TestCase):
    def test_I_and_streak_on_a_green_run(self):
        raw = [
            _day("d0", 1.00, 1.02, 0.99, 1.00),
            _day("d1", 1.00, 1.12, 0.99, 1.10),
            _day("d2", 1.10, 1.25, 1.08, 1.21),
            _day("d3", 1.21, 1.22, 0.95, 1.00),
        ]
        days = _hi_from_ohlc(raw)
        self.assertIsNone(days[0]["I"])
        self.assertAlmostEqual(days[1]["I"], 0.10, places=6)
        self.assertAlmostEqual(days[2]["I"], 0.10, places=6)
        self.assertTrue(days[3]["I"] < 0)
        g, r, *_ = _streaks(days)
        self.assertEqual(g[1], 0)
        self.assertEqual(g[2], 1)
        self.assertEqual(g[3], 2)
        self.assertEqual(r[3], 0)

    def test_hold_green_region_exits_on_first_red_I(self):
        raw = [
            _day("d0", 1.00, 1.02, 0.99, 1.00),
            _day("d1", 1.00, 1.12, 0.99, 1.10),
            _day("d2", 1.10, 1.25, 1.08, 1.21),
            _day("d3", 1.21, 1.22, 0.95, 1.00),
        ]
        days = _hi_from_ohlc(raw)
        got = _hold_to_region_end(days, 1, +1)
        self.assertEqual(got["hold_days"], 3)
        self.assertAlmostEqual(got["hold_open_to_last_close"], 0.00, places=6)

    def test_fwd_does_not_need_rows_above(self):
        raw = [
            _day("d0", 1, 1.1, 0.9, 1.0),
            _day("d1", 1, 1.2, 0.9, 1.1),
            _day("d2", 1.1, 1.3, 1.0, 1.2),
        ]
        days = _hi_from_ohlc(raw)
        o = _fwd(days, 1, 2)
        self.assertEqual(o["n"], 2)
        self.assertGreater(o["green_density"], 0.9)


class ClockAndMine(unittest.TestCase):
    def test_features_at_open_ignore_today_hlc(self):
        raw = [
            _day(f"d{i}", 10 + i * 0.1, 10.2 + i * 0.1, 9.9 + i * 0.1, 10.05 + i * 0.1)
            for i in range(12)
        ]
        raw[-1]["high"] = 999
        raw[-1]["low"] = 0.01
        raw[-1]["close"] = 999
        days = _hi_from_ohlc(raw)
        xl, flags = features_at_open(days, len(days) - 1)
        self.assertFalse(xl.get("same_row_df"))
        self.assertIn("df", xl)
        self.assertIsInstance(flags.get("prior_hammer"), bool)

    def test_deep_green_beats_uncond_on_a_trend(self):
        raw = []
        px = 10.0
        for i in range(40):
            nxt = px * 1.02 if i < 28 else px * 0.99
            o, c = px, nxt
            h, l = max(o, c) * 1.005, min(o, c) * 0.995
            raw.append(_day(f"d{i}", o, h, l, c))
            px = nxt
        result = mine_days_map({"SYN": raw}, min_n=5)
        by = {(r["rule"], r["bucket"]): r for r in result["rows"]}
        row = by.get(("g_streak>=5", "h5"))
        self.assertIsNotNone(row)
        self.assertGreater(row["lift_green"], 1.0)
        self.assertGreater(row["delta_mean_I"], 0.0)

    def test_iter_rows_never_passes_today_I_as_a_feature(self):
        raw = [
            _day(f"d{i}", 10, 10.3, 9.8, 10.1 + (0.05 if i % 2 == 0 else -0.04))
            for i in range(20)
        ]
        n = 0
        for t, cond, fixed, ends in iter_rows(raw):
            n += 1
            self.assertNotIn("I_today", cond)
            self.assertIn("uncond", cond)
        self.assertGreater(n, 5)


if __name__ == "__main__":
    unittest.main()
