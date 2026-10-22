import unittest
from .evaluate_market_disclosures import score


class AccuracyAccounting(unittest.TestCase):
    def test_extra_entities_misses_and_wrong_attachments_all_prevent_a_pass(self):
        gold={'category':'law','span':[0,10],'attachment':'issuer','direct':True}
        cases=[{'id':'one','sector':'Energy','expected':[gold]}]
        right={'category':'law','spans':[[0,10]],'attachment':'issuer','direct':True}
        self.assertTrue(score(cases,{'one':[right]})['passed'])
        for rows in ([],[right,{**right,'spans':[[12,20]]}],[{**right,'direct':False}]):
            self.assertFalse(score(cases,{'one':rows})['passed'])


if __name__=='__main__':unittest.main()
