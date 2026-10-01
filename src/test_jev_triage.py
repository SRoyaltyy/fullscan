import unittest
from unittest.mock import patch
from . import jev_triage as t
from .jev_gate import gate, make_state
from .jev_eval import score_sample
import datetime as dt


def answer(label, options, probability=.97, confidence=.9):
    return {"type": "choice", "choice": label, "confidence": confidence,
            "probabilities": {k: probability if k == label else (1-probability)/(len(options)-1) for k in options}}


def payload(info="fact", link="direct", probability=.97):
    return {"model": "test", "answers": {
        "information": answer(info, t.QUESTIONS["information"]["criteria"], probability),
        "market_link": answer(link, t.QUESTIONS["market_link"]["criteria"], probability)}}


class TriageTests(unittest.TestCase):
    def test_keep_and_noise(self):
        self.assertEqual(t.decide({}, payload())["decision"], "keep")
        self.assertEqual(t.decide({}, payload("noise"))["decision"], "drop")
    def test_abstention_is_retained_and_not_automatic(self):
        out = t.decide({}, payload(probability=.7))
        self.assertEqual((out["decision"], out["routing"]), ("keep", "review"))
    def test_malformed_probabilities_never_drop(self):
        for value in (float("nan"), float("inf"), -1, 2):
            p = payload(); p["answers"]["information"]["probabilities"]["fact"] = value
            self.assertEqual(t.decide({}, p)["reason"], "jev_error")
        self.assertEqual(t.decide({}, {"answers": {}})["routing"], "review")
    def test_teacher_only_on_uncertain_and_gets_evidence(self):
        calls = []
        def teacher(row): calls.append(row); return "drop"
        t.decide({"title": "test"}, payload(), teacher)
        self.assertEqual(calls, [])
        out = t.decide({"title": "test"}, payload(probability=.7), teacher)
        self.assertEqual(out["routing"], "teacher")
        self.assertEqual(calls, [{"title": "test"}])
    def test_both_live_policies_retain_request_failures(self):
        def fail(*args): raise RuntimeError("unavailable")
        for policy in ("sixbit", "triage"):
            out = gate([{"title": "Micron reports record earnings"}], live=True, key="test", poster=fail, policy=policy)[0]
            self.assertEqual((out["decision"],out["reason"]), ("keep", "jev_error"))
    def test_partial_sixbit_answer_is_error(self):
        out = gate([{"title": "Company announces acquisition"}], live=True, key="test", poster=lambda *a:{"answers":{"done":{"type":"noul","noul":.9}}})[0]
        self.assertEqual(out["reason"], "jev_error")
    def test_evaluation_refuses_errors_after_all_retries(self):
        with patch("src.jev_eval.time.sleep"):
            with self.assertRaises(RuntimeError):
                score_sample([{"title":"Micron reports record earnings"}],live=True,key="test",workers=1,poster=lambda *a:{},asof=dt.date.today())
    def test_only_news_context_enters_prompt(self):
        state = make_state({"title":"X", "snippet":"Actual earnings details", "grade":"K", "human_reason":"keep", "known_class":"earnings"})
        self.assertIn("Actual earnings details",state)
        self.assertNotIn("keep",state)
        self.assertNotIn("earnings\n", state)

if __name__ == "__main__": unittest.main()
