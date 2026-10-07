#!/usr/bin/env python3
"""Unit tests for src/recipe_scorecard.py (stdlib only).

Run: python3 src/test_recipe_scorecard.py
"""
from __future__ import annotations

import gzip
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import recipe_scorecard as rs  # noqa: E402


def ledger_day(day: str, recipes: dict[str, dict]) -> bytes:
    doc = {"date": day, "recipes": {}}
    for name, body in recipes.items():
        doc["recipes"][name] = {"primary": body, "starts": {}}
    return gzip.compress(json.dumps(doc).encode(), compresslevel=9, mtime=0)


def primary(equity: float, n_trades: int = 0) -> dict:
    return {"daily": {"equity": equity, "date": "x"},
            "trades": [{}] * n_trades}


class FoldTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        (self.root / "data/factor_mine/ledgers").mkdir(parents=True)

    def tearDown(self):
        self.tmp.cleanup()

    def write(self, day: str, recipes: dict[str, dict]):
        p = self.root / "data/factor_mine/ledgers" / f"{day}.json.gz"
        p.write_bytes(ledger_day(day, recipes))

    def test_fold_two_days(self):
        self.write("2026-08-13", {"a": primary(10000.0), "b": primary(10000.0, 2)})
        self.write("2026-08-14", {"a": primary(10100.0, 1), "b": primary(9900.0)})
        s = rs.fold_ledgers(self.root)
        self.assertEqual(s["a"], [("2026-08-13", 10000.0, 0), ("2026-08-14", 10100.0, 1)])
        self.assertEqual(s["b"], [("2026-08-13", 10000.0, 2), ("2026-08-14", 9900.0, 0)])

    def test_missing_primary_daily_skipped(self):
        self.write("2026-08-13", {"a": {}, "b": {"daily": {"equity": 10000.0}, "trades": []}})
        s = rs.fold_ledgers(self.root)
        self.assertEqual(list(s), ["b"])

    def test_unreadable_ledger_skipped(self):
        self.write("2026-08-13", {"a": primary(10000.0)})
        bad = self.root / "data/factor_mine/ledgers" / "2026-08-14.json.gz"
        bad.write_bytes(b"not gzip")
        s = rs.fold_ledgers(self.root)
        self.assertEqual(len(s["a"]), 1)

    def test_no_ledger_dir_fails(self):
        import shutil
        shutil.rmtree(self.root / "data")
        with self.assertRaises(SystemExit):
            rs.fold_ledgers(self.root)


class BuildTests(unittest.TestCase):
    def test_total_and_recent(self):
        days = [(f"2026-08-{13+i:02d}", 10000.0 + i * 100.0, 1) for i in range(25)]
        sc = rs.build_scorecard({"a": days})
        r = sc["recipes"]["a"]
        self.assertEqual(r["n_trades"], 25)
        # last equity 12400 vs 10000 baseline
        self.assertAlmostEqual(r["total_ret_pct"], 24.0, places=3)
        # recent: last equity vs equity 20 sessions earlier
        ref = days[-21][1]
        self.assertAlmostEqual(r["ret_recent_pct"], (days[-1][1] / ref - 1) * 100, places=3)
        self.assertEqual(sc["n_days"], 25)
        self.assertEqual(sc["start_date"], "2026-08-13")
        self.assertEqual(sc["last_date"], days[-1][0])

    def test_short_series_recent_equals_total(self):
        days = [("2026-08-13", 10000.0, 0), ("2026-08-14", 10200.0, 3)]
        r = rs.build_scorecard({"a": days})["recipes"]["a"]
        self.assertAlmostEqual(r["ret_recent_pct"], r["total_ret_pct"])

    def test_single_day_uses_baseline(self):
        r = rs.build_scorecard({"a": [("2026-08-13", 10500.0, 2)]})["recipes"]["a"]
        self.assertAlmostEqual(r["total_ret_pct"], 5.0)


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        led = self.root / "data/factor_mine/ledgers"
        led.mkdir(parents=True)
        (led / "2026-08-13.json.gz").write_bytes(ledger_day("2026-08-13", {"a": primary(10000.0)}))
        (led / "2026-08-14.json.gz").write_bytes(ledger_day("2026-08-14", {"a": primary(10300.0, 2)}))

    def tearDown(self):
        self.tmp.cleanup()

    def test_write_and_check_roundtrip(self):
        self.assertEqual(rs.main(["--root", str(self.root)]), 0)
        out = self.root / "data/factor_mine/recipe_scorecard.json"
        self.assertTrue(out.is_file())
        self.assertEqual(rs.main(["--root", str(self.root), "--check"]), 0)
        sc = json.loads(out.read_text())
        self.assertAlmostEqual(sc["recipes"]["a"]["total_ret_pct"], 3.0)

    def test_check_fails_when_stale(self):
        rs.main(["--root", str(self.root)])
        out = self.root / "data/factor_mine/recipe_scorecard.json"
        sc = json.loads(out.read_text())
        sc["recipes"]["a"]["total_ret_pct"] = 999.0
        out.write_text(json.dumps(sc))
        self.assertEqual(rs.main(["--root", str(self.root), "--check"]), 1)


if __name__ == "__main__":
    unittest.main(verbosity=1)
