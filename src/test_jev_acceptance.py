import unittest
from .jev_acceptance import score,summarize,sha,RUBRIC

def rows(offset=0,correct=100):
    return [{"title":f"Article {offset+i}","gold":"keep" if i<50 else "drop",
             "decision":("keep" if i<50 else "drop") if i<correct else "keep"} for i in range(100)]
def round_(offset=0,correct=100,version="v1"):
    return {"items":rows(offset,correct),"protocol_sha256":version,"rubric_sha256":sha(RUBRIC),
            "jev_model":"test","teacher_model":"frontier","teacher_blind":True}
class Tests(unittest.TestCase):
    def test_strict_threshold(self): self.assertFalse(score(rows(correct=90))["pass"])
    def test_five_fresh_rounds(self): self.assertTrue(summarize([round_(i*100) for i in range(5)])["accepted"])
    def test_repeat_does_not_count(self): self.assertFalse(summarize([round_() for _ in range(5)])["accepted"])
    def test_failure_resets(self): self.assertEqual(summarize([round_(0),round_(100,90),round_(200)])["streak"],1)
    def test_rubric_change_resets(self):self.assertEqual(summarize([round_(0),round_(100,version="v2")])["streak"],1)
    def test_review_is_wrong(self):
        r=rows()
        for x in r[:10]:x["review_required"]=True
        self.assertFalse(score(r)["pass"])
    def test_partial_batch_rejected(self):
        with self.assertRaises(ValueError):score(rows()[:99])
    def test_development_cannot_count(self):
        r=round_();r["dataset_role"]="development"
        self.assertFalse(summarize([r])["rounds"][0]["pass"])
    def test_unknown_model_cannot_count(self):
        r=round_();r["jev_model"]="unknown"
        self.assertFalse(summarize([r])["rounds"][0]["pass"])
    def test_duplicate_legacy_round_invalid_without_crash(self):
        r=round_();r["items"][-1]=dict(r["items"][-2])
        summary=summarize([r,round_(100)])
        self.assertFalse(summary["rounds"][0]["valid_protocol"])
        self.assertEqual(summary["streak"],1)
    def test_model_change_resets_streak(self):
        r=round_(100);r["jev_model"]="new-model"
        self.assertEqual(summarize([round_(),r])["streak"],1)
    def test_old_exposure_rejected(self):
        from .jev_acceptance import identity
        self.assertFalse(summarize([round_()],historical_seen=[identity(rows()[0])])["rounds"][0]["pass"])
if __name__=="__main__": unittest.main()
