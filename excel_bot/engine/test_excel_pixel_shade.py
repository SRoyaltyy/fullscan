"""Pale / mid / deep overlay is distinct from mixed _g."""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_pixel_shade import shade_of, shade_overlay, features_with_shade  # noqa: E402


def _day(fams, vals=None, texts=None, fills=None):
    return {
        "o": 10.0, "h": 10.4, "l": 9.7, "c": 10.2, "H": 0.07, "I": 0.01,
        "fams": fams, "vals": vals or {}, "fills": fills or {}, "texts": texts or {},
    }


class Cuts(unittest.TestCase):
    def test_cuts(self):
        self.assertEqual(shade_of("green", 1.0), "pale")
        self.assertEqual(shade_of("green", 1.5), "mid")
        self.assertEqual(shade_of("green", 2.0), "deepg")
        self.assertEqual(shade_of("green", 0.0), "")
        self.assertEqual(shade_of("red", -1.0), "pink")
        self.assertEqual(shade_of("red", -1.5), "midr")
        self.assertEqual(shade_of("red", -2.0), "deepr")


class Overlay(unittest.TestCase):
    def test_hex_pale_vs_deep(self):
        letters = list("ABCDEFGHIJKLMNO")
        days = []
        for i in range(12):
            fams = {let: "green" for let in letters}
            if i < 4:
                fills = {let: "DCEDD5" for let in letters}
            elif i < 8:
                fills = {let: "95CA82" for let in letters}
            else:
                fills = {let: "1B5E20" for let in letters}
            days.append(_day(fams, {"J": 0.02, "I": 0.01, "H": 0.06}, fills=fills))
        d = shade_overlay(days, 11)
        self.assertTrue(d.get("A@L1_deepg"))
        self.assertFalse(d.get("A@L1_pale"))
        self.assertFalse(d.get("A@L1_mid"))
        d3 = shade_overlay(days, 3)
        self.assertTrue(d3.get("A@L1_pale"))
        self.assertTrue(d3.get("A@L1_g_not_deep"))
        self.assertFalse(d3.get("A@L1_deepg"))
        self.assertNotIn("H@L0_pale", d3)
        self.assertNotIn("I@L0_mid", d3)

    def test_unknown_hex_is_not_deep(self):
        letters = list("ABCDEFGHIJKLMNO")
        days = [_day({let: "green" for let in letters}, {"J": 0.02}) for _ in range(6)]
        d = shade_overlay(days, 5)
        self.assertFalse(d.get("A@L1_deepg"))
        self.assertFalse(d.get("A@L1_pale"))
        self.assertFalse(d.get("A@L1_mid"))

    def test_merged_features_keep_g(self):
        letters = list("ABCDEFGHIJKLMNO")
        days = []
        for i in range(8):
            fams = {let: "green" for let in letters}
            fills = {let: "DCEDD5" for let in letters}
            days.append(_day(fams, {"J": 0.02, "I": 0.01, "H": 0.06}, fills=fills))
        d = features_with_shade(days, 6, {})
        self.assertTrue(d.get("A@L1_g") or d.get("A@L0_g"))
        self.assertTrue(d.get("A@L1_pale"))
        self.assertFalse(d.get("A@L1_deepg"))
        self.assertNotIn("H@L0_pos", d)
        self.assertNotIn("I@L0_pos", d)


if __name__ == "__main__":
    unittest.main()
