import unittest
from unittest.mock import patch
from test_semantic_headlines import valid
from src.news_impact.semantic_headlines import SemanticHeadlineClient
from src.news_impact.sector_batch_review import sector_prompt

def sectors(n=1,evidence='device'):
    return {'n':n,'sectors':[{'sector':'Healthcare','activity':'medical devices','entity':None,'basis':'headline_activity','evidence':evidence}],'unknowns':[]}

class SectorReviewTests(unittest.TestCase):
    def test_sector_pass_preserves_raw_classification_and_actions(self):
        draft=valid();draft['sectors']=[]
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=[({'results':[{'n':1,'analysis':draft}]},'glm-5.3-flash'),({'results':[sectors()]},'glm-5.3-flash')]) as ask:
            out=SemanticHeadlineClient(['zhipu'],review=False,sector_review=True).analyze_many([{'n':1,'title':'FDA approves device'}])[0]
        self.assertEqual(ask.call_count,2)
        self.assertIs(out['model_output'],draft)
        self.assertEqual(out['model_output']['sectors'],[])
        self.assertEqual(out['analysis_output']['primary_lane_class'],'gate')
        self.assertTrue(out['analysis_output']['actions'][0]['concrete_public_action'])
        self.assertEqual(out['analysis_output']['sectors'][0]['sector'],'Healthcare')
        second=ask.call_args_list[1].args[1]
        self.assertNotIn('LOCKED LANE DEFINITIONS',second)
        self.assertNotIn('DRAFT_TO_AUDIT',second)
        self.assertNotIn('reference',second.lower())

    def test_failed_sector_pass_is_not_a_valid_empty_sector(self):
        broken=sectors();broken['sectors'][0]['evidence']='invented evidence'
        responses=[({'results':[{'n':1,'analysis':valid()}]},'glm-5.3-flash'),({'results':[broken]},'glm-5.3-flash'),({'results':[broken]},'glm-5.3-flash')]
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=responses):
            out=SemanticHeadlineClient(['zhipu'],review=False,sector_review=True).analyze_many([{'n':1,'title':'FDA approves device'}])[0]
        self.assertEqual(out['status'],'unavailable')
        self.assertIsNone(out['model_output'])
        self.assertNotIn('analysis_output',out)
        self.assertEqual(out['draft_analysis']['status'],'analyzed')

    def test_only_missing_sector_rows_are_retried(self):
        a=valid();b=valid();b['events'][0]['evidence']=['FDA approves second device']
        responses=[({'results':[{'n':1,'analysis':a},{'n':2,'analysis':b}]},'glm-5.3-flash'),({'results':[sectors(1)]},'glm-5.3-flash'),({'results':[sectors(2)]},'glm-5.3-flash')]
        rows=[{'n':1,'title':'FDA approves device'},{'n':2,'title':'FDA approves second device'}]
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=responses) as ask:
            out=SemanticHeadlineClient(['zhipu'],review=False,sector_review=True).analyze_many(rows)
        self.assertTrue(all(r['status']=='analyzed' for r in out))
        import json
        retry=json.loads(ask.call_args_list[2].args[1].split('HEADLINES=',1)[1])
        self.assertEqual([r['n'] for r in retry],[2])

    def test_deadline_before_sector_pass_does_not_count_draft_success(self):
        class Deadline(BaseException):pass
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({'zhipu':'synthetic'},'', '')),patch('src.news_impact.semantic_headlines.route.ask_lane',side_effect=[({'results':[{'n':1,'analysis':valid()}]},'glm-5.3-flash'),Deadline()]):
            client=SemanticHeadlineClient(['zhipu'],review=False,sector_review=True)
            with self.assertRaises(Deadline):client.analyze_many([{'n':1,'title':'FDA approves device'}])
        self.assertEqual(client.batch_progress,{})

    def test_sector_prompt_is_smaller_than_full_classification(self):
        from src.news_impact.semantic_headlines import instructions
        self.assertLess(len(sector_prompt()),len(instructions())/2)
        self.assertIn('activity explicitly affected',sector_prompt())

if __name__=='__main__':unittest.main()


