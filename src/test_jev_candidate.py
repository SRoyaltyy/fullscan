"""Contract tests for the frozen taxonomy/atomic policy and safe routing."""
import copy,unittest
from . import jev_candidate as c
from .jev_acceptance import identity

def payload(news=.95,fact=.1,noise=.1):
    p={'model':'jev-1.13.0','answers':{}}
    p['answers']['kind']={'type':'choice','probabilities':{'company_news':news,'policy_data':0.,'industry_fact':0.,'noise':1-news},'choice':'company_news' if news>=.5 else 'noise'}
    for key in ('company','macro','industry','noise'):
        p['answers'][key]={'type':'noul','noul':noise if key=='noise' else fact}
    return p

class Candidate(unittest.TestCase):
    def classify(self,title,p):return c.decide({'title':title},p)['decision']
    def test_independent_atomic_recovers_factual_news(self):
        self.assertEqual(self.classify('Company cuts guidance',payload(.5,.8)), 'keep')
    def test_noise_veto_and_weak_evidence(self):
        self.assertEqual(self.classify('Generic investing advice',payload(.95,.9,.5)), 'drop')
        self.assertEqual(self.classify('Vague development',payload(.8,.6)), 'drop')
    def test_earnings_results_survive_reaction_wrapper(self):
        self.assertEqual(self.classify('Company stock falls after earnings beat and guidance cut',payload()),'keep')
    def test_transcript_preview_and_wrap_veto(self):
        for title in ['Company Q2 Earnings Call Highlights','Company to report Q2 earnings tomorrow','Stock Market Today: Dow Falls; Nvidia Beats Earnings']:
            self.assertEqual(self.classify(title,payload()),'drop')
    def test_missing_nonfinite_and_boolean_answer_rejected(self):
        for value in (float('nan'),float('inf'),True,-.1,1.1):
            p=payload();p['answers']['macro']['noul']=value
            with self.assertRaises(ValueError):c.decide({'title':'News'},p)
        p=payload();del p['answers']['company']
        with self.assertRaises(KeyError):c.decide({'title':'News'},p)
    def test_canonical_publisher_suffix(self):
        self.assertEqual(identity({'title':'Company raises guidance - Investor’s Business Daily'}),identity({'title':'Company raises guidance'}))

if __name__=='__main__':unittest.main()
