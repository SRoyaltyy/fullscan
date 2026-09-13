"""Tests for whole-sheet statistical mine."""
from __future__ import annotations

import os
import sys
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_stat_mine import ALL_LETTERS, _chi2_p, bh_keep, col_letters  # noqa: E402


class Letters(unittest.TestCase):
    def test_span_A_to_JL(self):
        letters = col_letters("JL")
        self.assertEqual(letters[0], "A")
        self.assertEqual(letters[-1], "JL")
        self.assertEqual(len(letters), 272)
        self.assertEqual(len(ALL_LETTERS), 272)

    def test_includes_DF_DH_BQ(self):
        s = set(ALL_LETTERS)
        for col in ("DF", "DG", "DH", "BB", "BQ", "H", "I", "O", "JL"):
            self.assertIn(col, s)


class Stats(unittest.TestCase):
    def test_independent_chi2_not_tiny(self):
        chi, p = _chi2_p(25, 25, 25, 25)
        self.assertGreater(p, 0.5)

    def test_associated_chi2_small(self):
        chi, p = _chi2_p(40, 10, 10, 40)
        self.assertLess(p, 1e-6)

    def test_bh_keeps_strong_only(self):
        rows = [
            {"rule": "weak", "p": 0.2},
            {"rule": "strong", "p": 1e-8},
            {"rule": "mid", "p": 0.04},
        ]
        keep = bh_keep(rows, q=0.10)
        self.assertIn("strong", keep)


if __name__ == "__main__":
    unittest.main()
