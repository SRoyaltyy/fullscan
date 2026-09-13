"""Tests for whole-sheet statistical mine + crash-resume checkpoint."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_stat_mine import (  # noqa: E402
    ALL_LETTERS, _atomic_json, _chi2_p, _ranked_from_counts, bh_keep,
    col_letters, load_ckpt,
)


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


class Checkpoint(unittest.TestCase):
    def test_atomic_roundtrip_and_resume(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "ckpt.json")
            payload = {
                "phase": "disc_uni",
                "processed": ["AAPL", "MSFT"],
                "n": 40,
                "lab_n": 22,
                "counts": {"I-1_g": [18, 30], "H-1_r": [4, 12]},
            }
            _atomic_json(path, payload)
            self.assertTrue(os.path.isfile(path))
            self.assertFalse(os.path.isfile(path + ".tmp"))
            got = load_ckpt(path)
            self.assertEqual(got["phase"], "disc_uni")
            self.assertEqual(got["processed"], ["AAPL", "MSFT"])
            self.assertEqual(got["counts"]["I-1_g"], [18, 30])

    def test_corrupt_ckpt_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "bad.json")
            open(path, "w").write("{not json")
            self.assertIsNone(load_ckpt(path))

    def test_partial_counts_still_rank(self):
        counts = {"deepg": [80, 100], "noise": [5, 10]}
        rows = _ranked_from_counts(counts, n=200, lab_n=100, min_n=20)
        self.assertEqual(rows[0]["rule"], "deepg")
        self.assertGreater(rows[0]["lift"], 1.0)


if __name__ == "__main__":
    unittest.main()
