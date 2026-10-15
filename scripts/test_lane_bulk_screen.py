import json,os,pathlib,tempfile,unittest
from unittest.mock import patch
from src.news_impact.bulk_screen import concrete,identity_keys,partition,request_once
from run_lane_bulk_screen import Ledger,run,point_form

class Catalog:
    def candidates(self,title):return []

def rows(size=100):return [{'n':n,'title':f'Court orders remedy {n}'} for n in range(1,size+1)]
def decision(row):return {'n':row['n'],'lane':'blast_legal','lane_evidence':'Court orders','actions':[{'actor_type':'court','kind':'decision','stage':'reported','evidence':'Court orders'}],'sectors':[],'why':'Court issued a remedy.','unknowns':[]}

class BulkTests(unittest.TestCase):
    def test_100_and_200_rows_each_use_exactly_one_subscription_request(self):
        for count in (100,200):
            data=rows(count);body={'choices':[{'message':{'content':json.dumps({'results':[decision(r) for r in data]})},'finish_reason':'stop'}],'usage':{'prompt_tokens':123,'completion_tokens':456}}
            with self.subTest(count=count),patch.dict(os.environ,{'OPENCLAW_GATEWAY_URL':'https://subscription.invalid','OPENCLAW_TOKEN':'test-only','OPENCLAW_AGENT':'openclaw/default'}),patch('src.news_impact.bulk_screen.route.http_json',return_value=(200,body,{})) as post:
                good,bad,meta=request_once(data,Catalog())
            self.assertEqual(post.call_count,1);self.assertEqual(len(good),count);self.assertFalse(bad)
            args=post.call_args.args;self.assertEqual(args[0],'https://subscription.invalid/v1/chat/completions')
            self.assertEqual(args[1]['model'],'openclaw/default');self.assertEqual(args[2]['x-openclaw-model'],'xai/grok-4.3')
            self.assertEqual(meta['provider_reported_usage']['completion_tokens'],456);self.assertIsNone(meta['weekly_allowance_remaining'])

    def test_gateway_rate_limit_has_no_json_repair_or_model_fallback(self):
        with patch.dict(os.environ,{'OPENCLAW_GATEWAY_URL':'https://subscription.invalid','OPENCLAW_TOKEN':'test-only'}),patch('src.news_impact.bulk_screen.route.http_json',return_value=(429,{'error':'limited'},{'Retry-After':'60'})) as post:
            good,bad,meta=request_once(rows(100),Catalog())
        self.assertEqual(post.call_count,1);self.assertFalse(good);self.assertEqual(len(bad),100)
        self.assertEqual(meta['quota_headers_if_exposed'],{'retry-after':'60'});self.assertEqual(meta['automatic_retries'],0)

    def test_new_article_budget_leaves_other_rows_unsubmitted(self):
        with tempfile.TemporaryDirectory() as d:
            ledger=Ledger(pathlib.Path(d)/'ledger.db','method')
            row=rows(1)[0]
            with patch('run_lane_bulk_screen.request_once',return_value=({1:decision(row)|{'concrete_public_action':True}},{},{'http_status':200,'http_requests':1})) as post:
                report=run(rows(2),ledger,Catalog(),max_new=1)
            self.assertEqual(post.call_count,1);self.assertEqual(report['not_visited'],1)
            self.assertEqual(report['outputs'][1]['status'],'not_submitted');ledger.close()

    def test_missing_row_retains_valid_rows_without_inventing_negative(self):
        good,bad=partition({'results':[decision(rows(2)[0])]},rows(2))
        self.assertEqual(set(good),{1});self.assertEqual(bad,{2:'missing_row'})

    def test_cross_title_quotes_and_duplicate_ids_fail_closed(self):
        data=rows(2);item=decision(data[0]);item['actions'][0]['evidence']='unreported decree'
        good,bad=partition({'results':[item,decision(data[1])]},data)
        self.assertEqual(set(good),{2});self.assertIn(1,bad)
        good,bad=partition({'results':[decision(data[0]),decision(data[0])]},data)
        self.assertFalse(good);self.assertEqual(set(bad),{1,2})

    def test_public_private_stage_and_uncertainty_preserve_existing_definition(self):
        act={'actor_type':'court','kind':'decision','stage':'reported'}
        self.assertTrue(concrete([act]));self.assertFalse(concrete([act|{'actor_type':'private_company'}]))
        self.assertFalse(concrete([act|{'stage':'future'}]));self.assertIsNone(concrete([act|{'actor_type':'unknown'}]))
        self.assertIsNone(concrete([act|{'stage':'uncertain'}]));self.assertTrue(concrete([act|{'actor_type':'unknown'},act]))

    def test_duplicate_identity_ignores_publisher_and_tracking_but_preserves_numbers(self):
        self.assertEqual(identity_keys({'title':'Court orders 12 remedies - Paper A'})[0],identity_keys({'title':'Court orders 12 remedies - Paper B'})[0])
        self.assertNotEqual(identity_keys({'title':'Court orders 12 remedies'})[0],identity_keys({'title':'Court orders 13 remedies'})[0])
        self.assertEqual(identity_keys({'title':'a','url':'http://example.com/a?utm_source=x&id=2#top'})[1],identity_keys({'title':'b','url':'https://example.com/a?id=2'})[1])

    def test_persistent_duplicates_across_processes_are_not_resubmitted(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'ledger.db';ledger=Ledger(path,'same-method')
            def complete(batch,*args):return {r['n']:decision(r)|{'concrete_public_action':True} for r in batch},{},{'http_status':200,'http_requests':1,'valid_rows':len(batch)}
            data=rows(2)+[{'n':3,'title':'Court orders remedy 1 - Other Publisher'}]
            with patch('run_lane_bulk_screen.request_once',side_effect=complete) as call:
                first=run(data,ledger,Catalog());self.assertEqual(call.call_count,1);self.assertEqual(len(call.call_args.args[0]),2)
            ledger.close();ledger=Ledger(path,'same-method')
            with patch('run_lane_bulk_screen.request_once') as call:
                second=run(data,ledger,Catalog());call.assert_not_called()
            self.assertEqual(first['submitted_unique_articles'],2);self.assertEqual(second['reused_input_rows'],3)
            self.assertIn('**3. YES**',point_form(second));ledger.close()

    def test_quota_failure_is_not_retried_or_cached_as_negative(self):
        with tempfile.TemporaryDirectory() as d:
            ledger=Ledger(pathlib.Path(d)/'ledger.db','method')
            with patch('run_lane_bulk_screen.request_once',return_value=({}, {1:'failed'},{'http_status':429,'http_requests':1})) as call:
                report=run(rows(2),ledger,Catalog(),batch_size=1)
            self.assertEqual(call.call_count,1);self.assertEqual(report['stop_reason'],'rate_limited')
            self.assertEqual(report['outputs'][0]['status'],'unavailable');self.assertEqual(report['outputs'][1]['status'],'not_submitted')
            with patch('run_lane_bulk_screen.request_once',return_value=({}, {2:'failed'},{'http_status':429,'http_requests':1})) as call:
                run(rows(2),ledger,Catalog(),batch_size=1)
            self.assertEqual(call.call_count,1);self.assertEqual(call.call_args.args[0][0]['n'],2);ledger.close()

    def test_crash_claim_is_not_automatically_resent_and_method_change_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            path=pathlib.Path(d)/'ledger.db';ledger=Ledger(path,'method');ledger.claim(rows(1)[0]);ledger.close()
            ledger=Ledger(path,'method')
            with patch('run_lane_bulk_screen.request_once') as call:report=run(rows(1),ledger,Catalog());call.assert_not_called()
            self.assertEqual(report['outputs'][0]['status'],'inflight');ledger.close()
            with self.assertRaisesRegex(ValueError,'mismatch'):Ledger(path,'changed')

if __name__=='__main__':unittest.main()
