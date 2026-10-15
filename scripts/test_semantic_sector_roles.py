import unittest
from unittest.mock import patch
from test_semantic_headlines import valid
from src.news_impact.semantic_headlines import SemanticHeadlineClient,lane_schema

TITLE='FDA approves Maker Y device after Bank X buys shares'

def draft():
    value=valid()
    value['events'][0]['evidence']=['FDA approves Maker Y device']
    return {k:v for k,v in value.items() if k in lane_schema()['properties']}

def facts():
    bank={'sector':'Financial','activity':'banking','entity':'Bank X','basis':'business_identity','evidence':'Bank X','role':'investor_or_lender','role_evidence':'Bank X buys shares'}
    maker={'sector':'Healthcare','activity':'medical devices','entity':'Maker Y','basis':'headline_activity','evidence':'device','role':'target_business','role_evidence':'FDA approves Maker Y device'}
    return {'results':[{'n':1,'coverage':{'institutional_predicates':['FDA approves'],'business_subjects':['Maker Y','Bank X']},'sectors':[bank,maker],'actions':valid()['actions'],'unknowns':[]}]}

class SectorRoleTests(unittest.TestCase):
    def run_review(self,blob):
        responses=[({'results':[{'n':1,'analysis':draft()}]},'glm-5.3-flash'),(blob,'glm-5.3-flash'),(blob,'glm-5.3-flash')]
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=responses) as ask:
            out=SemanticHeadlineClient(['zhipu'],review=False,action_review=True,sector_roles=True).analyze_many([{'n':1,'title':TITLE}])[0]
        return out,ask.call_count

    def test_role_filter_preserves_raw_and_other_deliverables_in_two_calls(self):
        blob=facts()
        out,calls=self.run_review(blob)
        self.assertEqual(calls,2)
        self.assertEqual([s['sector'] for s in out['analysis_output']['sectors']],['Healthcare'])
        self.assertEqual(len(out['fact_review']['model_output']['sectors']),2)
        self.assertEqual(out['fact_review']['model_output']['sectors'][0]['role'],'investor_or_lender')
        self.assertEqual(out['analysis_output']['primary_lane_class'],'gate')
        self.assertTrue(out['analysis_output']['actions'][0]['concrete_public_action'])

    def assert_failed_role(self,blob):
        out,_=self.run_review(blob)
        self.assertEqual(out['status'],'unavailable')
        self.assertIsNone(out['model_output'])
        self.assertNotIn('analysis_output',out)
        self.assertEqual(out['draft_analysis']['status'],'analyzed')

    def test_missing_role_does_not_become_valid_empty_sector(self):
        blob=facts();del blob['results'][0]['sectors'][0]['role']
        self.assert_failed_role(blob)

    def test_role_quote_must_belong_to_own_headline(self):
        blob=facts();blob['results'][0]['sectors'][0]['role_evidence']='invented purchase'
        self.assert_failed_role(blob)

    def test_unknown_role_is_rejected(self):
        blob=facts();blob['results'][0]['sectors'][0]['role']='trusted_override'
        self.assert_failed_role(blob)

    def test_role_mode_cannot_add_full_audit(self):
        with patch('src.news_impact.semantic_headlines.route.load_keys') as keys:
            with self.assertRaisesRegex(ValueError,'sector_roles requires review=False'):
                SemanticHeadlineClient(review=True,sector_roles=True)
        keys.assert_not_called()

if __name__=='__main__':unittest.main()
