"""Pixel factory covers A..JO and does not emit same-row H/I."""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_pixel_desc import ALL_LETTERS, pixel_features  # noqa: E402


def _day(fams, vals=None, texts=None):
    return {
        "o": 10.0, "h": 10.4, "l": 9.7, "c": 10.2, "H": 0.07, "I": 0.01,
        "fams": fams, "vals": vals or {}, "fills": {}, "texts": texts or {},
    }


class Span(unittest.TestCase):
    def test_A_to_JO(self):
        self.assertEqual(ALL_LETTERS[0], "A")
        self.assertEqual(ALL_LETTERS[-1], "JO")
        self.assertEqual(len(ALL_LETTERS), 275)
        self.assertIn("DF", ALL_LETTERS)
        self.assertIn("DH", ALL_LETTERS)


class Pixels(unittest.TestCase):
    def test_emits_camera_and_bands(self):
        days = []
        for i in range(25):
            fams = {let: ("green" if i % 2 == 0 else "red")
                    for let in list("ABCDEFGHIJKLMNO")}
            vals = {"J": 0.01 * i, "C": 10 + i, "H": 0.02 if i % 3 else -0.04,
                    "I": 0.01 if i % 2 == 0 else -0.02, "G": 1.2 + i * 0.1,
                    "B": 10 + i}
            days.append(_day(fams, vals))
        d = pixel_features(days, 20, {"J": (-0.02, -0.01, 0, 0.01, 0.03)})
        self.assertTrue(d.get("A@L1_g") or d.get("A@L1_r"))
        self.assertTrue(any(k.startswith("band_AO@L1") for k in d))
        self.assertTrue(any(k.startswith("camera@L1") for k in d))
        self.assertTrue(any(k.startswith("cols_A") for k in d))
        self.assertNotIn("H@L0_pos", d)
        self.assertNotIn("I@L0_pos", d)
        self.assertTrue(any(k.startswith("Iregion_") or k.startswith("HI@") for k in d))

    def test_streak_and_window(self):
        days = []
        for i in range(20):
            fams = {let: "green" for let in list("ABCDEFGHIJKLMNO")}
            days.append(_day(fams, {"J": 0.02, "I": 0.01, "H": 0.06}))
        d = pixel_features(days, 15, {})
        self.assertTrue(d.get("A@L1_gstreak>=5") or d.get("A@L0_gstreak>=5"))
        self.assertTrue(d.get("Iregion_g>=5") or d.get("Iregion_deepg"))

    def test_candles_and_exists(self):
        days = []
        for i in range(8):
            fams = {let: "green" for let in list("ABCDEFGHIJKLMNO")}
            texts = {"DF": "Bullish Hammer", "DG": "Bullish Engulfing", "DH": "None"}
            vals = {"J": 0.01, "AH": 2, "FR": 1, "C": 10.0, "B": 9.8}
            days.append(_day(fams, vals, texts))
        d = pixel_features(days, 6, {})
        self.assertTrue(d.get("DF@L1=Bullish Hammer") or d.get("DF@L1_hammer"))
        self.assertTrue(d.get("candles@L1_bull") or d.get("DF@L1_bull"))
        self.assertTrue(d.get("AH@L0==2") or d.get("AH@L1==2") or d.get("AH@L0_exists")
                        or d.get("AH@L0_missing") or d.get("AH@L1_pos"))
        self.assertNotIn("DF@L0=Bullish Hammer", d)


if __name__ == "__main__":
    unittest.main()
