"""Learning-source contract tests; synthetic fixtures, never live auth."""
import json,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from . import finviz_learning_capture as c

class CaptureContract(unittest.TestCase):
    def test_summary_and_headline_are_not_full(self):
        html='<html><table class="snapshot-table2"><tr><td>Daily Digest</td><td>Stock rises on earnings</td></tr></table><table class="news-table"><a>Great earnings raise shares</a></table></html>'
        self.assertEqual(c.parse_full_panel(html,'A','Stock rises on earnings')['status'],'full_source_structure_unverified')
    def test_full_panel_preserves_every_paragraph(self):
        first='Demand rose after the company raised its outlook; several customers expanded their purchases.'
        second='Costs still matter. Supplier prices increased, while customer contracts only reset next quarter.'
        text='<section id="daily-digest"><h2>Daily Digest</h2><p>'+first+'</p><p>'+second+'</p></section>'
        d=c.parse_full_panel(text,'A','Outlook rises')
        self.assertEqual(d['status'],'full_panel_captured');self.assertEqual(d['full_text'],first+'\n\n'+second)
    def test_explicit_empty_differs_from_missing_panel(self):
        self.assertEqual(c.parse_full_panel('<section id="daily-digest"><p>No daily digest available</p></section>','A')['status'],'source_explicitly_no_digest')
        self.assertEqual(c.parse_full_panel('<html><h1>Company</h1></html>','A')['status'],'full_source_structure_unverified')
    def test_teaser_equal_to_export_cannot_pass(self):
        text='A long source summary '+('about revenue and costs '*8)
        self.assertNotEqual(c.parse_full_panel('<section id="daily-digest"><p>'+text+'</p></section>','A',text)['status'],'full_panel_captured')
    def test_csv_missing_digest_and_duplicate_tickers_fail(self):
        with self.assertRaises(ValueError):c.validate_csv(b'Ticker,Sector\nA,Technology\n')
        with self.assertRaises(ValueError):c.validate_csv(b'Ticker,Sector,Industry,Daily Digest\nA,Technology,Software,x\nA,Technology,Software,y\n')
    def test_same_text_keeps_two_ticker_records(self):
        common={'Company':'Test','Sector':'Technology','Industry':'Semiconductors','Daily Digest':'same words'}
        a=c.export_record({**common,'Ticker':'A'},'run',{});b=c.export_record({**common,'Ticker':'B'},'run',{})
        self.assertEqual(a['export_text_sha256'],b['export_text_sha256']);self.assertNotEqual(a['ticker'],b['ticker'])
    def test_no_digest_cell_not_verified_no_full_panel(self):
        html='<table class="snapshot-table2"><td>Daily Digest</td><td>-</td></table>'
        self.assertEqual(c.parse_full_panel(html,'A')['status'],'full_source_structure_unverified')
    def test_http_429_stops_without_retry_and_does_not_log_auth(self):
        class S:
            calls=0
            def get(self,url,timeout):
                self.calls+=1
                return type('R',(),{'status_code':429})()
        s=S()
        with patch.object(c.finviz_session,'_pace'):
            r=c.page_request(s,'A')
        self.assertEqual(r['status'],'rate_limited');self.assertEqual(s.calls,1)
    def test_failed_full_capture_retains_all_tickers_and_fails(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);dest=root/'data/capture';dest.mkdir(parents=True)
            manifest={'capture_id':'run','export':{'origin':'live_http'},'missing_expected_from_live_export':[],'prepare_error':None,'expected_universe':'fixture'}
            c.write_json(dest/'manifest.json',manifest)
            rows=[{'ticker':t,'capture_id':'run','export_status':'short_summary_received','full_status':'not_attempted','export_captured_at':'2026-10-05T17:00:00-04:00'} for t in ['A','B']]
            c.write_rows(dest/'baseline.jsonl.gz',rows)
            args=type('Args',(),{'root':root,'capture_path':'data/capture','mode':'full','phase':'auto'})()
            self.assertFalse(c.aggregate(args));saved=c.read_rows(dest/'records.jsonl.gz')
            self.assertEqual(len(saved),2);self.assertEqual({r['full_status'] for r in saved},{'capture_shard_missing'})
    def test_export_mode_cannot_certify_full(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);dest=root/'run';dest.mkdir()
            c.write_json(dest/'manifest.json',{'capture_id':'x','export':{'origin':'live_http'},'missing_expected_from_live_export':[],'prepare_error':None,'expected_universe':'fixture'})
            c.write_rows(dest/'baseline.jsonl.gz',[{'ticker':'A','capture_id':'x','export_status':'blank_in_export_unresolved','full_status':'not_attempted','export_captured_at':'2026-10-05T17:00:00-04:00'}])
            a=type('Args',(),{'root':root,'capture_path':'run','mode':'export','phase':'auto'})()
            self.assertTrue(c.aggregate(a));d=json.loads((dest/'coverage.json').read_text());self.assertFalse(d['full_text_complete'])
    def test_requested_preopen_rejects_late_capture(self):
        self.assertEqual(c.phase_at('2026-10-05T14:47:49-04:00'),'intraday')
    def test_paywall_or_expand_teaser_cannot_pass(self):
        text='An incomplete explanation '+('sales rose and costs changed '*8)
        html='<section id="daily-digest"><p>'+text+'</p><button>Show full digest</button></section>'
        self.assertEqual(c.parse_full_panel(html,'A')['status'],'full_source_structure_unverified')
    def test_failed_live_export_does_not_relabel_cached_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'data/universe').mkdir(parents=True);(root/'data/exports').mkdir()
            (root/'data/universe/2026-10-05_membership.csv').write_text('Ticker,sector,industry\nA,Technology,Semiconductors\nB,Technology,Semiconductors\n')
            cached=root/'data/exports/finviz_latest.csv';cached.write_text('old cached data')
            a=type('Args',(),{'root':root,'date':'2026-10-05','capture_id':'one','mode':'full','phase':'auto','shards':16})()
            with patch.object(c,'now',return_value='2026-10-05T17:00:00-04:00'),patch.object(c.finviz_session,'session',return_value=object()),patch.object(c,'live_export',side_effect=ValueError('no_live_export_with_daily_digest_column')),patch.dict('os.environ',{'GITHUB_OUTPUT':''}):
                c.prepare(a)
            records=c.read_rows(root/'data/finviz_learning/2026-10-05/one/baseline.jsonl.gz')
            self.assertEqual(len(records),2);self.assertEqual({r['export_status'] for r in records},{'live_export_failed'})
            self.assertEqual(cached.read_text(),'old cached data')
    def test_403_stops_rest_of_batch_and_checkpoints_remaining(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);dest=root/'run';dest.mkdir()
            c.write_json(dest/'manifest.json',{'capture_id':'x','full_source_verified_in_this_run':True,'shards':1})
            rows=[{'ticker':t,'capture_id':'x','export_text':'summary','full_status':'not_attempted'} for t in ['A','B','C']]
            c.write_rows(dest/'baseline.jsonl.gz',rows)
            a=type('Args',(),{'root':root,'capture_path':'run','shard':0,'budget_minutes':1})()
            result={'status':'access_denied','captured_at':'2026-10-05T17:00:00-04:00'}
            with patch.object(c.finviz_session,'session',return_value=object()),patch.object(c.finviz_session,'authed',return_value=True),patch.object(c,'page_request',return_value=result) as request:
                c.shard(a)
            self.assertEqual(request.call_count,1)
            saved=c.read_rows(dest/'shards/00.jsonl.gz');self.assertEqual([r['full_status'] for r in saved],['access_denied','not_attempted_access_denied','not_attempted_access_denied'])
    def test_refuse_overwriting_existing_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'data/finviz_learning/2026-10-05/one').mkdir(parents=True)
            a=type('Args',(),{'root':root,'date':'2026-10-05','capture_id':'one'})()
            with patch.object(c,'now',return_value='2026-10-05T17:00:00-04:00'):
                with self.assertRaisesRegex(ValueError,'already_exists'):c.prepare(a)
if __name__=='__main__':unittest.main()
