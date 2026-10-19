"""Self-tests for the eval harness. Run from the repo root:

    PYTHONPATH=. python3 -m unittest src.test_eval_harness

These tests use synthetic program output only — no filings archive needed.
"""
from __future__ import annotations

import unittest

from src.eval_correlation import compare_case, summarize
from src.eval_diff import diff_rounds
from src.eval_sample import classify_bucket, sample

WORD_TICKERS_PLACES = {"charlotte", "new york", "north carolina"}


def _hit(status="scored", stocks=None, dependencies=None):
    return {"id": "x", "status": status, "stocks": stocks or [], "dependencies": dependencies or []}


def _row(ticker, pass_=False, name_hit=False, ind_pass=None, desc_pass=None):
    row = {"ticker": ticker, "pass": pass_, "name_hit": name_hit}
    if ind_pass is not None:
        row["ind_pass"] = ind_pass
    if desc_pass is not None:
        row["desc_pass"] = desc_pass
    return row


class CompareCaseTests(unittest.TestCase):
    def test_all_expectations_met_is_pass(self):
        case = {"id": "c", "kind": "regression",
                "expected": {"status": "scored", "topical_passes": ["RKLB"],
                             "must_not_pass": ["AAL"], "name_hits": ["DKNG"], "must_not_name": ["POST"]},
                "text": "a long enough article body to pass the forty character gate"}
        hit = _hit(stocks=[_row("RKLB", pass_=True, ind_pass=True, desc_pass=True),
                           _row("AAL"), _row("DKNG", name_hit=True), _row("POST")])
        out = compare_case(case, hit)
        self.assertEqual(out["verdict"], "PASS")
        self.assertEqual(out["failure_classes"], [])

    def test_wrong_stock_passing_is_false_pass(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"must_not_pass": ["AAL"]}}
        out = compare_case(case, _hit(stocks=[_row("AAL", pass_=True)]))
        self.assertEqual(out["verdict"], "FAIL")
        self.assertIn("FALSE_PASS", out["failure_classes"])

    def test_present_but_gated_is_gate_miss(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"topical_passes": ["BA"]}}
        out = compare_case(case, _hit(stocks=[_row("BA", ind_pass=False, desc_pass=True)]))
        self.assertIn("GATE_MISS", out["failure_classes"])

    def test_present_but_desc_failed_is_threshold(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"topical_passes": ["BA"]}}
        out = compare_case(case, _hit(stocks=[_row("BA", ind_pass=True, desc_pass=False)]))
        self.assertIn("THRESHOLD", out["failure_classes"])

    def test_absent_expected_stock_is_false_unknown(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"topical_passes": ["BA"]}}
        out = compare_case(case, _hit(stocks=[_row("AAL")]))
        self.assertIn("FALSE_UNKNOWN", out["failure_classes"])
        self.assertEqual(out["missing"]["topical_passes"], ["BA"])

    def test_expected_name_hit_absent_is_missing_name(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"name_hits": ["LMT"]}}
        out = compare_case(case, _hit(stocks=[_row("LMT")]))
        self.assertIn("MISSING_NAME", out["failure_classes"])

    def test_word_ticker_flagged_is_alias_fp(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"must_not_name": ["POST"]}}
        out = compare_case(case, _hit(stocks=[_row("POST", name_hit=True)]))
        self.assertIn("ALIAS_FP", out["failure_classes"])

    def test_unexpected_name_hit_is_alias_fp(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"no_name_hits": True}}
        out = compare_case(case, _hit(stocks=[_row("RLJ", name_hit=True)]))
        self.assertIn("ALIAS_FP", out["failure_classes"])

    def test_ungated_allowed_dependency_is_relation_leak(self):
        case = {"id": "c", "kind": "target", "text": "x" * 60,
                "expected": {"allowed_second_order": [{"ticker": "PLBY", "partner": "DraftKings"}]}}
        dep = {"ticker": "PLBY", "role": "collaborator", "partner": "DraftKings", "section": "s"}
        out = compare_case(case, _hit(dependencies=[dep]))
        self.assertEqual(out["verdict"], "TARGET_FAIL")
        self.assertIn("RELATION_LEAK", out["failure_classes"])

    def test_marked_second_order_dependency_passes_target(self):
        case = {"id": "c", "kind": "target", "text": "x" * 60,
                "expected": {"allowed_second_order": [{"ticker": "PLBY", "partner": "DraftKings"}]}}
        dep = {"ticker": "PLBY", "role": "collaborator", "partner": "DraftKings",
               "section": "s", "order": 2}
        out = compare_case(case, _hit(dependencies=[dep]))
        self.assertEqual(out["verdict"], "TARGET_ACHIEVED")

    def test_forbidden_dependency_is_relation_leak(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"forbidden_dependencies": ["CHDN"]}}
        dep = {"ticker": "CHDN", "role": "customer", "partner": "DraftKings", "section": "s"}
        out = compare_case(case, _hit(dependencies=[dep]))
        self.assertIn("RELATION_LEAK", out["failure_classes"])

    def test_unknown_status_when_scored_expected(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"status": "scored", "topical_passes": ["RKLB"]}}
        out = compare_case(case, _hit(status="unknown"))
        self.assertIn("FALSE_UNKNOWN", out["failure_classes"])

    def test_unknown_status_when_unknown_expected_passes(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"status": "unknown"}}
        out = compare_case(case, _hit(status="unknown"))
        self.assertEqual(out["verdict"], "PASS")

    def test_scored_but_nothing_linked_is_acceptable_unknown(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"status": "unknown"}}
        out = compare_case(case, _hit(status="scored", stocks=[_row("AAL")]))
        self.assertEqual(out["verdict"], "PASS")
        self.assertTrue(out["notes"])

    def test_empty_expectations_is_unlabeled(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60, "expected": {}}
        out = compare_case(case, _hit())
        self.assertEqual(out["verdict"], "UNLABELED")

    def test_missing_text_skips(self):
        case = {"id": "c", "kind": "regression", "text": "", "expected": {"status": "scored"}}
        out = compare_case(case, _hit())
        self.assertEqual(out["verdict"], "SKIP")
        self.assertIn("NO_TEXT", out["failure_classes"])

    def test_short_text_with_unknown_expectation_is_scorable(self):
        # thin-markets-moved is 14 characters and expects unknown. It must
        # count, not SKIP on the 40-character floor.
        case = {"id": "thin-markets-moved", "kind": "regression", "text": "Markets moved.",
                "expected": {"status": "unknown"}}
        out = compare_case(case, _hit(status="unknown"))
        self.assertEqual(out["verdict"], "PASS")
        self.assertNotIn("NO_TEXT", out["failure_classes"])

    def test_missing_hit_row_is_false_unknown(self):
        case = {"id": "c", "kind": "regression", "text": "x" * 60,
                "expected": {"status": "scored"}}
        out = compare_case(case, None)
        self.assertEqual(out["verdict"], "FAIL")
        self.assertIn("FALSE_UNKNOWN", out["failure_classes"])

    def test_summarize_counts_classes_only_on_failures(self):
        results = [
            {"verdict": "PASS", "kind": "regression", "failure_classes": []},
            {"verdict": "FAIL", "kind": "regression", "failure_classes": ["GATE_MISS", "ALIAS_FP"]},
            {"verdict": "TARGET_FAIL", "kind": "target", "failure_classes": ["RELATION_LEAK"]},
            {"verdict": "UNLABELED", "kind": "regression", "failure_classes": []},
        ]
        summary = summarize(results)
        self.assertEqual(summary["regression_pass_rate"], 0.5)
        self.assertEqual(summary["targets_achieved"], "0/1")
        self.assertEqual(summary["unlabeled"], 1)
        self.assertEqual(summary["failure_classes"], {"GATE_MISS": 1, "ALIAS_FP": 1, "RELATION_LEAK": 1})


def _round(generated, cases):
    return {"generated": generated, "changelog": "", "summary": summarize(cases), "cases": cases}


class DiffTests(unittest.TestCase):
    def test_regression_flip_sets_exit_condition(self):
        old = _round("t0", [{"case_id": "a", "kind": "regression", "verdict": "PASS", "failure_classes": []}])
        new = _round("t1", [{"case_id": "a", "kind": "regression", "verdict": "FAIL", "failure_classes": ["FALSE_PASS"]}])
        report = diff_rounds(old, new)
        self.assertEqual(len(report["regressions"]), 1)
        self.assertEqual(report["failure_class_delta"], {"FALSE_PASS": 1})

    def test_improvement_and_target_achievement(self):
        old = _round("t0", [
            {"case_id": "a", "kind": "regression", "verdict": "FAIL", "failure_classes": ["GATE_MISS"]},
            {"case_id": "b", "kind": "target", "verdict": "TARGET_FAIL", "failure_classes": ["RELATION_LEAK"]},
        ])
        new = _round("t1", [
            {"case_id": "a", "kind": "regression", "verdict": "PASS", "failure_classes": []},
            {"case_id": "b", "kind": "target", "verdict": "TARGET_ACHIEVED", "failure_classes": []},
        ])
        report = diff_rounds(old, new)
        self.assertEqual(report["regressions"], [])
        self.assertEqual(len(report["improvements"]), 1)
        self.assertEqual(len(report["targets_achieved"]), 1)
        self.assertEqual(report["failure_class_delta"], {"GATE_MISS": -1, "RELATION_LEAK": -1})

    def test_removed_case_is_flagged(self):
        old = _round("t0", [{"case_id": "a", "kind": "regression", "verdict": "PASS", "failure_classes": []}])
        new = _round("t1", [])
        report = diff_rounds(old, new)
        self.assertEqual(report["removed_cases"], ["a"])


class SamplerTests(unittest.TestCase):
    def test_bucket_classification(self):
        places = WORD_TICKERS_PLACES
        self.assertEqual(classify_bucket({"title": "Markets moved.", "body": ""}, places, set()), "thin")
        self.assertEqual(classify_bucket({"title": "Tesla buys more coin machines", "body": "x" * 300}, places, set()), "ticker_word")
        self.assertEqual(classify_bucket({"title": "Gold falls - Fana News", "body": "x" * 300}, places, set()), "publisher")
        self.assertEqual(classify_bucket({"title": "Charlotte police probe", "body": "x" * 300}, places, set()), "place")
        self.assertEqual(classify_bucket({"title": "Gambling ruling nears", "body": "DraftKings investors waited. " + "x" * 300}, places, {"DraftKings"}), "partner")
        self.assertEqual(classify_bucket({"title": "Fed holds rates", "body": "x" * 300}, places, set()), "random")

    def test_sample_respects_budget_and_stratifies(self):
        rows = (
            [{"title": f"Thin {i}", "body": ""} for i in range(4)]
            + [{"title": f"Charlotte story {i}", "body": "y" * 300} for i in range(4)]
            + [{"title": f"Random story {i}", "body": "z" * 300} for i in range(20)]
        )
        picked = sample(rows, budget=10, relations=set(), seed=7)
        self.assertLessEqual(len(picked), 10)
        self.assertTrue(any("Charlotte" in r["title"] for r in picked))
        self.assertTrue(any("Thin" in r["title"] for r in picked))
        self.assertTrue(any("Random" in r["title"] for r in picked))


if __name__ == "__main__":
    unittest.main()
