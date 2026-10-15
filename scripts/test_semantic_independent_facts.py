import unittest
from unittest.mock import patch
from test_semantic_headlines import valid
from src.news_impact.semantic_headlines import SemanticHeadlineClient,lane_schema,lane_instructions,instructions
from src.news_impact.sector_batch_review import action_prompt
from run_semantic_headlines import no_available_credentials

def lane():
    return {k:v for k,v in valid().items() if k in lane_schema()['properties']}

def facts(include_actions=True):
    result={'n':1,'sectors':valid()['sectors'],'unknowns':[]}
    if include_actions:result.update(actions=valid()['actions'],coverage={'institutional_predicates':['FDA approves'],'business_subjects':['device']})
    return {'results':[result]}

class IndependentFactsTests(unittest.TestCase):
    def test_two_independent_stages_preserve_raw_and_derive_actions(self):
        draft=lane()
        responses=[({'results':[{'n':1,'analysis':draft}]},'glm-5.3-flash'),(facts(),'glm-5.3-flash')]
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=responses) as ask:
            out=SemanticHeadlineClient(['zhipu'],review=False,action_review=True).analyze_many([{'n':1,'title':'FDA approves device'}])[0]
        self.assertEqual(ask.call_count,2)
        self.assertIs(out['model_output'],draft)
        self.assertNotIn('actions',out['model_output'])
        self.assertNotIn('sectors',out['model_output'])
        self.assertEqual(out['analysis_output']['primary_lane_class'],'gate')
        self.assertTrue(out['analysis_output']['actions'][0]['concrete_public_action'])
        self.assertEqual(out['analysis_output']['sectors'][0]['sector'],'Healthcare')
        self.assertEqual(out['fact_review']['status'],'analyzed')
        second=ask.call_args_list[1].args[1]
        self.assertNotIn('LOCKED LANE DEFINITIONS',second)
        self.assertNotIn('DRAFT_TO_AUDIT',second)

    def test_missing_required_actions_is_unavailable_not_negative(self):
        responses=[({'results':[{'n':1,'analysis':lane()}]},'glm-5.3-flash'),(facts(False),'glm-5.3-flash'),(facts(False),'glm-5.3-flash')]
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=responses):
            out=SemanticHeadlineClient(['zhipu'],review=False,action_review=True).analyze_many([{'n':1,'title':'FDA approves device'}])[0]
        self.assertEqual(out['status'],'unavailable')
        self.assertIsNone(out['model_output'])
        self.assertNotIn('analysis_output',out)
        self.assertEqual(out['draft_analysis']['status'],'analyzed')

    def test_coverage_is_preserved_as_evidence_without_changing_labels(self):
        blob=facts()
        responses=[({'results':[{'n':1,'analysis':lane()}]},'glm-5.3-flash'),(blob,'glm-5.3-flash')]
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=responses) as ask:
            out=SemanticHeadlineClient(['zhipu'],review=False,action_review=True).analyze_many([{'n':1,'title':'FDA approves device'}])[0]
        self.assertEqual(ask.call_count,2)
        self.assertEqual(out['fact_review']['model_output']['coverage'],blob['results'][0]['coverage'])
        self.assertNotIn('coverage',out['analysis_output'])
        self.assertTrue(out['analysis_output']['actions'][0]['concrete_public_action'])

    def test_missing_or_cross_title_coverage_fails_closed(self):
        import copy
        original=facts()
        variants=[]
        absent=copy.deepcopy(original);del absent['results'][0]['coverage'];variants.append(absent)
        wrong=copy.deepcopy(original);wrong['results'][0]['coverage']['business_subjects']=['another row company'];variants.append(wrong)
        malformed=copy.deepcopy(original);malformed['results'][0]['coverage']['institutional_predicates']='FDA approves';variants.append(malformed)
        for blob in variants:
            with self.subTest(blob=blob):
                responses=[({'results':[{'n':1,'analysis':lane()}]},'glm-5.3-flash'),(blob,'glm-5.3-flash'),(blob,'glm-5.3-flash')]
                with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=responses):
                    out=SemanticHeadlineClient(['zhipu'],review=False,action_review=True).analyze_many([{'n':1,'title':'FDA approves device'}])[0]
                self.assertEqual(out['status'],'unavailable')
                self.assertNotIn('analysis_output',out)

    def test_full_audit_cannot_add_a_third_call(self):
        with patch('src.news_impact.semantic_headlines.route.load_keys') as keys:
            with self.assertRaisesRegex(ValueError,'requires review=False'):
                SemanticHeadlineClient(review=True,action_review=True)
        keys.assert_not_called()

    def test_lane_schema_is_small_and_action_rules_are_separate(self):
        self.assertEqual(set(lane_schema()['properties']),{'events','primary_lane_class','primary_why','unknowns'})
        self.assertLess(len(lane_instructions()),len(instructions()))
        self.assertIn('sentencing',action_prompt())

    def test_second_stage_failure_does_not_stop_as_missing_credentials(self):
        failed={'status':'unavailable','attempts':[],'fact_review':{'attempts':[{'status':'schema_rejected'}]}}
        self.assertFalse(no_available_credentials([failed]))
        self.assertFalse(no_available_credentials([]))
        self.assertTrue(no_available_credentials([{'status':'unavailable','attempts':[{'status':'credential_unavailable'}]}]))

if __name__=='__main__':unittest.main()
