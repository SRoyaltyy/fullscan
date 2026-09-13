"""Pixel factory covers A..JO and does not emit same-row H/I."""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_pixel_desc import ALL_LETTERS, pixel_features, _shade  # noqa: E402


def _day(fams, vals=None, texts=None, fills=None):
    return {
        "o": 10.0, "h": 10.4, "l": 9.7, "c": 10.2, "H": 0.07, "I": 0.01,
        "fams": fams, "vals": vals or {}, "fills": fills or {}, "texts": texts or {},
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
        # no hex on these days → intensity unknown → shade flags stay off
        self.assertFalse(d.get("A@L1_pale"))
        self.assertFalse(d.get("A@L1_mid"))
        self.assertFalse(d.get("A@L1_deepg"))

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


class Shade(unittest.TestCase):
    def test_shade_cuts(self):
        self.assertEqual(_shade("green", 1.0), "pale")
        self.assertEqual(_shade("green", 1.5), "mid")
        self.assertEqual(_shade("green", 2.0), "deepg")
        self.assertEqual(_shade("green", 0.0), "")
        self.assertEqual(_shade("red", -1.0), "pink")
        self.assertEqual(_shade("red", -1.5), "midr")
        self.assertEqual(_shade("red", -2.0), "deepr")

    def test_hex_pale_vs_mid_vs_deep(self):
        letters = list("ABCDEFGHIJKLMNO")
        days = []
        for i in range(12):
            fams = {let: "green" for let in letters}
            if i < 4:
                fills = {let: "DCEDD5" for let in letters}   # pale
            elif i < 8:
                fills = {let: "95CA82" for let in letters}   # mid
            else:
                fills = {let: "1B5E20" for let in letters}   # deep
            days.append(_day(fams, {"J": 0.02, "I": 0.01, "H": 0.06}, fills=fills))
        d = pixel_features(days, 11, {})
        self.assertTrue(d.get("A@L1_g"))
        self.assertTrue(d.get("A@L1_deepg"))
        self.assertFalse(d.get("A@L1_pale"))
        self.assertFalse(d.get("A@L1_mid"))
        self.assertTrue(d.get("A@L1_g_not_deep") is not True)
        self.assertTrue(d.get("A@L5_mid") or d.get("M@L5_mid"))
        d3 = pixel_features(days, 3, {})
        self.assertTrue(d3.get("A@L1_pale"))
        self.assertFalse(d3.get("A@L1_deepg"))
        self.assertTrue(d3.get("A@L1_g_not_deep"))
        self.assertTrue(d3.get("Iregion_pale>=2") or d3.get("A@L1_palestreak>=2"))

    def test_unknown_intensity_is_not_deep(self):
        letters = list("ABCDEFGHIJKLMNO")
        days = [_day({let: "green" for let in letters}, {"J": 0.02}) for _ in range(6)]
        d = pixel_features(days, 5, {})
        self.assertTrue(d.get("A@L1_g"))
        self.assertFalse(d.get("A@L1_deepg"))
        self.assertFalse(d.get("A@L1_pale"))
        self.assertFalse(d.get("A@L1_mid"))


if __name__ == "__main__":
    unittest.main()
