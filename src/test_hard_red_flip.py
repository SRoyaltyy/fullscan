"""Hard-red polarity flip — opposite side at the 09:30 open.

Run: PYTHONPATH=. python3 -m src.test_hard_red_flip
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

from src import factor_mine_combo as fmc
from src import hard_red_flip as hf
from src.test_hard_red_sit_research import (
    DATES,
    ZERO_FEES,
    _combo_panel,
    _recs,
)


class FlipSweep(unittest.TestCase):
    def test_flip_side_inverts_long_and_short(self):
        self.assertEqual(hf.flip_side("long"), "short")
        self.assertEqual(hf.flip_side("short"), "long")
        self.assertEqual(hf.flip_side(None), "short")

    def test_live_hard_red_mode_is_still_sit_not_flip(self):
        self.assertTrue(fmc.hard_red_skip_new("long", "sit"))
        self.assertTrue(fmc.hard_red_skip_new("short", "sit"))
        self.assertNotIn("flip", fmc.HARD_RED_MODES)
        self.assertNotIn("polarity_flip", fmc.HARD_RED_MODES)

    def test_open_day_fire_long_uses_open_not_low(self):
        cal = ["2026-08-13", "2026-08-14"]
        bars = {
            ("AAA", "2026-08-13"): {
                "open": 10.0, "high": 10.2, "low": 9.0, "close": 10.5,
            },
            ("AAA", "2026-08-14"): {
                "open": 10.4, "high": 10.6, "low": 10.3, "close": 11.0,
            },
        }
        row = hf.open_day_fire(
            "AAA", "2026-08-13", cal, bars=bars, fees=ZERO_FEES,
            side="long", hold=2)
        self.assertEqual(row["kind"], "open_flip")
        self.assertAlmostEqual(row["entry"], 10.0)
        self.assertAlmostEqual(row["exit"], 11.0)
        self.assertTrue(row["win"])
        self.assertEqual(row["intended"], "short")

    def test_run_flips_combo_kids_on_hard_red(self):
        panel, bars, regime = _combo_panel()
        payload = hf.run(
            from_date=DATES[0], to_date=DATES[-1], write=False,
            panel=panel, bars=bars, fees=ZERO_FEES, regime=regime,
            recipes=_recs())
        self.assertGreaterEqual(payload["hard_red_n"], 1)
        self.assertTrue(payload["live_sit"])
        names = {s["name"] for s in payload["strategies"]}
        self.assertIn("short_news_r_macd_h3", names)
        short = next(s for s in payload["strategies"]
                     if s["name"] == "short_news_r_macd_h3")
        self.assertEqual(short["side"], "short")
        self.assertEqual(short["fired_side"], "long")
        long = next(s for s in payload["strategies"]
                    if s["name"] == "union_hot_n4_h1")
        self.assertEqual(long["fired_side"], "short")
        if short.get("picked"):
            self.assertIn(short["picked"]["hold"], hf.HOLD_GRID)
            self.assertEqual(short["picked"]["mode"], "polarity_flip")


if __name__ == "__main__":
    unittest.main()
