"""Contract tests for src/lesson_exec — the apply + grade half of the
learning loop, plus the ranker/learner gates that must respect it.

Run: PYTHONPATH=. python3 -m src.test_lesson_exec
"""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from src import lesson_exec
from src.lesson_exec import apply_to_frame

LESSON_MD = """---
trigger_pattern: "t"
corrected_behavior: "c"
falsifier: "f"
current_behavior: "cb"
evidence_cited: "e"
error_category: "A"
scope: "book"
status: "active"
---

## RULE
Do the thing.

## WHEN IT FIRES
t

## WRONG IF
f

(learn_cycle promote)
"""


def _frame() -> pd.DataFrame:
    return pd.DataFrame([
        # rescue candidate: join/sector hot, AB/peer still crash-negative
        {"Ticker": "CAPR", "s_join": 0.61, "s_sector": 0.60, "s_news": 0.0,
         "s_ab": -0.76, "s_peer": -0.73, "size": "small", "market_cap_m": 900.0},
        # micro admit + join-floor candidate: maxed peer, positive AB, bad join
        {"Ticker": "CYPH", "s_join": -0.15, "s_sector": 0.10, "s_news": 0.0,
         "s_ab": 0.70, "s_peer": 1.00, "size": "micro", "market_cap_m": 120.0},
        # earnings anti-fade candidate (earnings map mocked in tests)
        {"Ticker": "CRM", "s_join": -0.64, "s_sector": 0.20, "s_news": -0.46,
         "s_ab": 0.92, "s_peer": 0.10, "size": "mega", "market_cap_m": 250000.0},
        # plain name — nothing should fire
        {"Ticker": "PLAIN", "s_join": 0.10, "s_sector": 0.10, "s_news": 0.0,
         "s_ab": 0.10, "s_peer": 0.10, "size": "mid", "market_cap_m": 3000.0},
    ])


class ApplyCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        lessons = root / "lessons"
        lessons.mkdir()
        (lessons / "l1.md").write_text(LESSON_MD, encoding="utf-8")
        self.registry = root / "registry.json"
        patches = [
            mock.patch.object(lesson_exec, "LESSON_DIR", lessons),
            mock.patch.object(lesson_exec, "REGISTRY_PATH", self.registry),
            mock.patch.object(lesson_exec, "STATE_PATH", root / "state.json"),
            mock.patch.object(lesson_exec, "_earnings_map", lambda date: {}),
            mock.patch.object(lesson_exec, "_digest_map", lambda date: {}),
        ]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.addCleanup(self.tmp.cleanup)

    def write_registry(self, overrides: dict) -> None:
        self.registry.write_text(
            json.dumps({"overrides": overrides}), encoding="utf-8")

    def row(self, df, ticker):
        return df.loc[df["Ticker"] == ticker].iloc[0]

    def test_crash_rebound_admit_clips_and_admits(self):
        self.write_registry({"crash_rebound_admit": {
            "enabled": True, "lessons": ["l1.md"], "params": {}}})
        df, meta = apply_to_frame(_frame(), "2026-09-15")
        capr = self.row(df, "CAPR")
        self.assertEqual(capr["s_ab_lx"], 0.0)
        self.assertEqual(capr["s_peer_lx"], 0.0)
        self.assertTrue(bool(capr["lesson_admit"]))
        self.assertIn("crash_rebound_admit", capr["lesson_fires"])
        # raw columns untouched for audit
        self.assertEqual(capr["s_ab"], -0.76)
        plain = self.row(df, "PLAIN")
        self.assertFalse(bool(plain["lesson_admit"]))
        # rescue fired on CAPR, micro admit fired on CYPH
        self.assertEqual(meta["overrides"]["crash_rebound_admit"]["fired"], 2)

    def test_micro_admit_bypasses_size_gate_flag(self):
        self.write_registry({"crash_rebound_admit": {
            "enabled": True, "lessons": ["l1.md"], "params": {}}})
        df, _ = apply_to_frame(_frame(), "2026-09-15")
        cyph = self.row(df, "CYPH")
        self.assertTrue(bool(cyph["lesson_admit_micro"]))
        self.assertTrue(bool(cyph["lesson_admit"]))

    def test_join_floor_pass_clips_negative_join(self):
        self.write_registry({"join_floor_pass": {
            "enabled": True, "lessons": ["l1.md"], "params": {}}})
        df, _ = apply_to_frame(_frame(), "2026-09-15")
        cyph = self.row(df, "CYPH")
        self.assertEqual(cyph["s_join_lx"], 0.0)
        self.assertTrue(bool(cyph["lesson_admit"]))

    def test_earnings_anti_fade_clips_join_and_news(self):
        with mock.patch.object(
                lesson_exec, "_earnings_map", lambda date: {"CRM": 3}):
            self.write_registry({"earnings_anti_fade": {
                "enabled": True, "lessons": ["l1.md"], "params": {}}})
            df, _ = apply_to_frame(_frame(), "2026-09-15")
        crm = self.row(df, "CRM")
        self.assertEqual(crm["s_join_lx"], 0.0)
        self.assertEqual(crm["s_news_lx"], 0.0)
        self.assertEqual(crm["s_news"], -0.46)  # raw untouched

    def test_news_attach_and_neutralize(self):
        with mock.patch.object(
                lesson_exec, "_earnings_map", lambda date: {"CRM": 2}), \
             mock.patch.object(
                lesson_exec, "_digest_map", lambda date: {"CAPR": 1.6}):
            self.write_registry({"news_attach": {
                "enabled": True, "lessons": ["l1.md"], "params": {}}})
            df, _ = apply_to_frame(_frame(), "2026-09-15")
        capr = self.row(df, "CAPR")
        self.assertAlmostEqual(capr["s_news_lx"], 0.2)
        crm = self.row(df, "CRM")
        self.assertEqual(crm["s_news_lx"], 0.0)  # neg news treated as missing
        # news_attach is a score fix, not an eligibility admit
        self.assertFalse(bool(capr["lesson_admit"]))

    def test_suspended_override_is_noop(self):
        self.write_registry({"crash_rebound_admit": {
            "enabled": True, "lessons": ["l1.md"], "params": {}}})
        lesson_exec.STATE_PATH.write_text(json.dumps(
            {"overrides": {"crash_rebound_admit": {"suspended": True}}}),
            encoding="utf-8")
        df, meta = apply_to_frame(_frame(), "2026-09-15")
        capr = self.row(df, "CAPR")
        self.assertEqual(capr["s_ab_lx"], -0.76)
        self.assertFalse(bool(capr["lesson_admit"]))
        self.assertTrue(meta["overrides"]["crash_rebound_admit"]["suspended"])

    def test_missing_registry_is_neutral(self):
        # registry file never written → apply adds neutral columns only
        df, meta = apply_to_frame(_frame(), "2026-09-15")
        self.assertEqual(meta["overrides"], {})
        self.assertEqual(self.row(df, "CAPR")["s_ab_lx"], -0.76)
        self.assertIn("s_join_lx", df.columns)

    def test_retired_lesson_disables_override(self):
        lesson_exec.LESSON_DIR.joinpath("l1.md").unlink()
        self.write_registry({"crash_rebound_admit": {
            "enabled": True, "lessons": ["l1.md"], "params": {}}})
        df, meta = apply_to_frame(_frame(), "2026-09-15")
        self.assertEqual(self.row(df, "CAPR")["s_ab_lx"], -0.76)
        self.assertEqual(meta["overrides"]["crash_rebound_admit"]["fired"], 0)


class VerdictCase(unittest.TestCase):
    def test_thresholds(self):
        self.assertEqual(lesson_exec._verdict(6, 0.01, False), "working")
        self.assertEqual(lesson_exec._verdict(5, 0.0, False), "falsified")
        self.assertEqual(lesson_exec._verdict(5, -0.02, False), "falsified")
        self.assertEqual(lesson_exec._verdict(2, -0.05, False), "collecting")
        self.assertEqual(lesson_exec._verdict(0, 0.0, False),
                         "no triggers realized yet")
        self.assertEqual(lesson_exec._verdict(9, 0.5, True), "suspended")


class RankerGateCase(unittest.TestCase):
    """The live gates must respect the flags apply_to_frame sets."""

    def _book_df(self, admit_micro: bool) -> pd.DataFrame:
        return pd.DataFrame([
            {"Ticker": "CYPH", "sector": "Technology", "industry": "Software",
             "size": "micro", "market_cap_m": 120.0, "score_1w": 5.0,
             "lesson_admit_micro": admit_micro},
            {"Ticker": "MID", "sector": "Technology", "industry": "Hardware",
             "size": "mid", "market_cap_m": 3000.0, "score_1w": 1.0,
             "lesson_admit_micro": False},
        ])

    def test_book_side_micro_gate_respects_flag(self):
        from src import stock_book
        buys, _ = stock_book._book_side(self._book_df(True), "1w", 10)
        self.assertIn("CYPH", buys["Ticker"].tolist())
        buys2, _ = stock_book._book_side(self._book_df(False), "1w", 10)
        self.assertNotIn("CYPH", buys2["Ticker"].tolist())

    def test_buy_veto_waives_lag_leg_for_admitted(self):
        from src import stock_book
        df = pd.DataFrame([
            {"Ticker": "CAPR", "s_sector": 0.6, "s_peer": -0.7,
             "context_label": "LAG", "reasons": "", "lesson_admit": True},
            {"Ticker": "LAG2", "s_sector": 0.6, "s_peer": -0.7,
             "context_label": "LAG", "reasons": "", "lesson_admit": False},
        ])
        veto = stock_book._buy_veto_mask(df)
        self.assertFalse(bool(veto.iloc[0]))
        self.assertTrue(bool(veto.iloc[1]))

    def test_book_learn_components_prefer_lx(self):
        from src import book_learn
        df = pd.DataFrame([
            {"Ticker": "A", "s_join": 0.1, "s_join_lx": 0.9, "s_sector": 0.0,
             "s_general": 0.0, "s_news": 0.0, "s_ab": 0.0, "s_peer": 0.0},
        ])
        comp = book_learn._components_for(df, "1w")
        self.assertAlmostEqual(float(comp[0][0]), 0.9)

    def test_book_learn_select_buys_respects_micro_flag(self):
        import numpy as np
        from src import book_learn
        base = {
            "sector": "Technology", "industry": "Software",
            "size": "micro", "market_cap_m": 120.0,
        }
        df = pd.DataFrame([
            {**base, "Ticker": "CYPH", "lesson_admit_micro": True},
            {"Ticker": "MID", "sector": "Technology", "industry": "Hardware",
             "size": "mid", "market_cap_m": 3000.0, "lesson_admit_micro": False},
        ])
        score = np.array([5.0, 1.0])
        picks = book_learn._select_buys(df, score, 10)
        self.assertIn(0, picks)
        df.loc[0, "lesson_admit_micro"] = False
        picks2 = book_learn._select_buys(df, score, 10)
        self.assertNotIn(0, picks2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
