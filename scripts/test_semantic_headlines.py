import unittest
from unittest.mock import patch
from src.news_impact.semantic_headlines import SemanticHeadlineClient,validation_errors

def valid():return {'events':[{'event_type':'regulatory approval','lane_class':'gate','stage':'reported','evidence':['FDA approves device']}], 'actions':[{'actor':'FDA','actor_evidence':'FDA','action':'approves','action_evidence':'approves','object':'device','stage':'reported','public_authority':True,'concrete_public_action':True,'jurisdiction':'US'}], 'sectors':[{'sector':'Healthcare','activity':'medical devices','entity':None,'basis':'headline_activity','evidence':'device'}],'primary_lane_class':'gate','primary_why':'Reported regulatory permission','unknowns':['implementation unknown']}

class SemanticTests(unittest.TestCase):
    def test_literal_evidence_and_shape(self):
        self.assertFalse(validation_errors(valid(),'FDA approves device'))
        v=valid();v['actions'][0]['actor_evidence']='agency'
        self.assertTrue(validation_errors(v,'FDA approves device'))
        v=valid();v['sectors'][0]['sector']='AI winners'
        self.assertTrue(validation_errors(v,'FDA approves device'))
    def test_future_is_not_concrete(self):
        v=valid();v['actions'][0]['stage']='future'
        self.assertTrue(validation_errors(v,'FDA approves device'))
    def test_bool_is_not_string(self):
        v=valid();v['actions'][0]['concrete_public_action']='true'
        self.assertTrue(validation_errors(v,'FDA approves device'))
    def test_unavailable_is_not_negative(self):
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({},'', '')):
            r=SemanticHeadlineClient(['zhipu']).analyze('FDA approves device')
        self.assertEqual(r['status'],'unavailable');self.assertIsNone(r['model_output'])
    def test_success_retains_raw_model_output(self):
        v=valid()
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',return_value=(v,'glm-4.7-flash')):
            r=SemanticHeadlineClient(['zhipu']).analyze('FDA approves device')
        self.assertIs(r['model_output'],v)
    def test_opt_in_existing_annotation_contract(self):
        from src.news_impact.headline_analysis import analyse_headline
        class Client:
            def analyze(self,title):return {'status':'analyzed','model_output':valid()}
        row=analyse_headline('FDA approves device',semantic_client=Client())
        self.assertTrue(row['institutional_actions']['has_concrete_reported_action'])
        self.assertEqual(row['sector_classification']['market_sectors'],['Healthcare'])
        self.assertEqual(row['headline_event']['lane_class'],'gate')
    def test_adapter_does_not_turn_unavailability_into_negative(self):
        from src.news_impact.semantic_adapter import annotations_from_semantic
        row=annotations_from_semantic('A headline',{'status':'unavailable','model_output':None})
        self.assertIsNone(row['institutional_actions']['has_concrete_reported_action'])
        self.assertEqual(row['headline_event']['status'],'unavailable')

    def test_primary_requires_event_evidence(self):
        v=valid();v['events']=[]
        self.assertTrue(validation_errors(v,'FDA approves device'))

if __name__=='__main__':unittest.main()
