"""Tests for excel_deep_corr_mine — clock + filter + descriptors."""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_deep_corr_mine import (  # noqa: E402
    AVGVOL_MIN,
    MCAP_MIN_M,
    _count_fam,
    _hi,
    descriptors_at,
    load_finviz_filter,
)


def _day(date, o, h, l, c, fams=None):
    fills = ["#00aa00" if f == "green" else "#aa0000" if f == "red" else "#888888"
             for f in (fams or ["none"] * 15)]
    while len(fills) < 15:
        fills.append("#888888")
    return {"date": date, "open": o, "high": h, "low": l, "close": c,
            "volume": 200_000, "fills": fills}


class Filter(unittest.TestCase):
    def test_constants(self):
        self.assertEqual(MCAP_MIN_M, 50.0)
        self.assertEqual(AVGVOL_MIN, 100_000.0)

    def test_missing_finviz_is_empty(self):
        self.assertEqual(load_finviz_filter("/no/such.csv"), {})


class Descriptors(unittest.TestCase):
    def test_window_counts_ignore_today(self):
        raw = []
        for i in range(12):
            fams = ["green"] * 15 if i < 10 else ["red"] * 15
            raw.append(_day(f"d{i}", 10, 10.2, 9.8, 10.1, fams))
        days = _hi(raw)
        g = _count_fam(days, 10, 0, 10, "green")
        self.assertEqual(g, 10)
        self.assertEqual(days[10]["fams"][0], "red")

    def test_open_fill_keys_exist(self):
        raw = [_day(f"d{i}", 10 + i * 0.1, 10.3, 9.8, 10.2, ["green"] * 15)
               for i in range(25)]
        days = _hi(raw)
        d = descriptors_at(days, 20, {}, {})
        self.assertTrue(d["uncond"])
        self.assertIn("fill0_A_green", d)
        self.assertIn("A_last10_g>=6", d)
        self.assertNotIn("fill0_H_green", d)


if __name__ == "__main__":
    unittest.main()
