"""Tests for whole-sheet statistical mine + crash-resume checkpoint."""
from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import date, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)

from excel_stat_mine import (  # noqa: E402
    ALL_LETTERS, _chi2_p, bh_keep, col_letters, collect, disc_label_ok,
    discovery_quantiles, iso_date, label_end_date, label_horizon,
    normalize_days, time_split_cutoff,
)
from excel_stat_mine_ckpt import _atomic_json, _ranked_from_counts, load_ckpt  # noqa: E402
from excel_deep_corr_mine import (  # noqa: E402
    _disc_outcome_ok, _fwd, _hi, _hold_region, _time_slot, mine,
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
                "split_kind": "time",
                "cutoff": "2024-06-01",
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

    def test_ticker_split_ckpt_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "old.json")
            _atomic_json(path, {
                "phase": "pairs_disc",
                "processed": ["AAPL"],
                "n": 100,
                "confirmed": [{"rule": "M@L5_gstreak==1", "lift": 1.12}],
            })
            self.assertIsNone(load_ckpt(path))

    def test_missing_cutoff_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "half.json")
            _atomic_json(path, {"phase": "disc_uni", "split_kind": "time"})
            self.assertIsNone(load_ckpt(path))


def _days(start, n, green=True):
    d0 = date.fromisoformat(start)
    out = []
    px = 10.0
    for i in range(n):
        d = d0 + timedelta(days=i)
        c = px + (0.1 if green else -0.1)
        out.append({
            "date": d.isoformat(),
            "open": px, "high": max(px, c) + 0.05, "low": min(px, c) - 0.05,
            "close": c, "volume": 200_000,
            "fills": ["#00aa00"] * 15,
            "values": {"C": 1.0 + i * 0.01, "B": 1.0},
        })
        px = c
    return out


class TimeSplit(unittest.TestCase):
    def test_iso_date_serial_and_string(self):
        self.assertEqual(iso_date("2024-06-15"), "2024-06-15")
        self.assertEqual(iso_date(date(2024, 6, 15)), "2024-06-15")
        # 2024-06-15 Excel serial
        self.assertEqual(iso_date(45458), "2024-06-15")
        self.assertEqual(iso_date(None), "")
        self.assertEqual(iso_date("not-a-date"), "")

    def test_cutoff_is_first_holdout_date(self):
        days = _days("2020-01-01", 10)
        ticker_days = {"AAA": days}
        dates = [d["date"] for d in days]
        cutoff = time_split_cutoff(ticker_days, hold_frac=0.30)
        # 10 dates, 30% hold → first holdout index 7
        self.assertEqual(cutoff, dates[7])
        self.assertTrue(all(d < cutoff for d in dates[:7]))
        self.assertTrue(all(d >= cutoff for d in dates[7:]))

    def test_locked_cutoff_reused(self):
        days = _days("2020-01-01", 10)
        self.assertEqual(
            time_split_cutoff({"AAA": days}, locked="2020-01-05"),
            "2020-01-05",
        )

    def test_i1_ok_i5_spill_rejected(self):
        days = normalize_days(_days("2020-01-01", 40))
        cutoff = days[28]["date"]
        # t=24: I1 end = day 24 < cutoff; I5 end = day 28 == cutoff → reject I5
        self.assertEqual(label_horizon("I1_green"), 1)
        self.assertEqual(label_horizon("I5_green"), 5)
        feat = days[24]["date"]
        self.assertTrue(disc_label_ok(feat, label_end_date(days, 24, 1), cutoff))
        self.assertFalse(disc_label_ok(feat, label_end_date(days, 24, 5), cutoff))
        # t=20: I5 end = day 24 < cutoff
        self.assertTrue(disc_label_ok(days[20]["date"], label_end_date(days, 20, 5), cutoff))

    def test_collect_does_not_mix_disc_and_hold(self):
        raw = _days("2020-01-01", 50)
        ticker_days = {"AAA": raw, "BBB": raw}
        cutoff = time_split_cutoff(ticker_days, hold_frac=0.30)
        quant = discovery_quantiles(ticker_days, cutoff)
        disc = collect(ticker_days, cutoff, quant, "disc", "I1_green")
        hold = collect(ticker_days, cutoff, quant, "hold", "I1_green")
        self.assertGreater(len(disc), 0)
        self.assertGreater(len(hold), 0)
        # Feature dates live on normalized days; reconstruct from raw.
        nd = normalize_days(raw)
        disc_ok = {d["date"] for d in nd if d["date"] and d["date"] < cutoff}
        hold_ok = {d["date"] for d in nd if d["date"] and d["date"] >= cutoff}
        # I1 label end == feature date, so disc dates must all be < cutoff
        # and hold dates >= cutoff. collect doesn't return dates; check via
        # a dated walk of the same filter.
        from excel_stat_mine import label_end_date as led
        disc_dates, hold_dates = [], []
        for t in range(20, len(nd)):
            feat = nd[t]["date"]
            end = led(nd, t, 1)
            if disc_label_ok(feat, end, cutoff):
                disc_dates.append(feat)
            elif feat and feat >= cutoff:
                hold_dates.append(feat)
        self.assertTrue(disc_dates)
        self.assertTrue(hold_dates)
        self.assertTrue(all(d < cutoff for d in disc_dates))
        self.assertTrue(all(d >= cutoff for d in hold_dates))
        self.assertEqual(set(disc_dates) & set(hold_dates), set())
        self.assertTrue(set(disc_dates) <= disc_ok)
        self.assertTrue(set(hold_dates) <= hold_ok)

    def test_quantiles_ignore_holdout_dates(self):
        # Discovery C values stay near 1; holdout C jumps to 100.
        disc = _days("2020-01-01", 70)
        hold = _days("2020-03-11", 20)
        for d in hold:
            d["values"]["C"] = 100.0
        ticker_days = {"AAA": disc + hold}
        cutoff = "2020-03-11"
        q = discovery_quantiles(ticker_days, cutoff)
        self.assertIn("C", q)
        p25, p50, p75 = q["C"]
        self.assertLess(p75, 10.0)

    def test_deep_corr_i10_spill_not_in_discovery(self):
        raw = _days("2020-01-01", 45)
        days = _hi(raw)
        cutoff = days[32]["date"]
        t = 24
        feat = days[t]["date"]
        h1 = _fwd(days, t, 1)
        h10 = _fwd(days, t, 10)
        holdg = _hold_region(days, t, +1)
        self.assertEqual(_time_slot(feat, cutoff), "disc")
        self.assertTrue(_disc_outcome_ok(feat, h1, cutoff))
        self.assertFalse(_disc_outcome_ok(feat, h10, cutoff))
        # green streak walks to the last bar, which is after cutoff
        self.assertFalse(_disc_outcome_ok(feat, holdg, cutoff))

    def test_deep_corr_mine_uses_cutoff_not_names(self):
        raw = _days("2020-01-01", 50)
        cutoff = time_split_cutoff({"AAA": raw, "BBB": raw}, hold_frac=0.30)
        result = mine({"AAA": raw, "BBB": raw}, split_map={"AAA": "discovery", "BBB": "holdout"},
                      min_disc=5, min_hold=5, cutoff=cutoff)
        self.assertEqual(result["split_kind"], "time")
        self.assertEqual(result["cutoff"], cutoff)
        # uncond h1 should have both sides populated from the same names
        base = result["base"]["h1"]
        self.assertIsNotNone(base["disc"])
        self.assertIsNotNone(base["hold"])
        self.assertGreater(base["disc"]["n"], 0)
        self.assertGreater(base["hold"]["n"], 0)


if __name__ == "__main__":
    unittest.main()
