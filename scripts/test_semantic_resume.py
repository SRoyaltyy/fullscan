import copy,json,pathlib,tempfile,unittest
from unittest.mock import patch
from test_semantic_headlines import valid
from src.news_impact.semantic_headlines import SemanticHeadlineClient,lane_schema,rate_limit_only
from run_semantic_headlines import restore_checkpoint,pending_rows,resumable_drafts,batch_rate_limited,write_checkpoint

def rate():
    return {'status':'unavailable','model_output':None,'provider':None,'model':None,'attempts':[{'status':'transport_diagnostics','http_codes':['429']}],'rejected_outputs':[],'elapsed_seconds':0}

def record(raw):
    return {'status':'analyzed','model_output':raw,'provider':'zhipu','model':'glm-5.3-flash','attempts':[],'rejected_outputs':[],'elapsed_seconds':0}

def lane():
    return {k:v for k,v in valid().items() if k in lane_schema()['properties']}

def setup_checkpoint():
    rows=[{'n':1,'title':'one'},{'n':2,'title':'two'}]
    expected={'input_sha256':'input','runtime_sha256':{'runtime':'hash'},'configuration':{'models':('model',)},'catalog':{'sha256':'catalog'},'requested':2,'generated_at':'new','outputs':[]}
    old=copy.deepcopy(expected);old['configuration']['models']=['model'];old['generated_at']='original'
    old['outputs']=[{'n':1,'title':'one','program_output':{'status':'analyzed','model_output':{},'analysis_output':{}}},{'n':2,'title':'two','program_output':rate()}]
    old['stop_reason']='rate_limited'
    return rows,expected,old

class ResumeAndRateTests(unittest.TestCase):
    def test_resume_preserves_first_execution_and_only_retries_incomplete(self):
        rows,current,old=setup_checkpoint()
        result=restore_checkpoint(old,current,rows)
        self.assertEqual(result['generated_at'],'original')
        self.assertEqual([r['n'] for r in pending_rows(rows,result['outputs'])],[2])
        self.assertNotIn('stop_reason',result)
        self.assertEqual(result['resume_history'][0]['preserved_analyzed'],1)

    def test_candidate_input_catalog_or_limit_change_rejects_resume(self):
        rows,current,old=setup_checkpoint()
        for key in ('input_sha256','runtime_sha256','configuration','catalog','requested'):
            with self.subTest(key=key):
                altered=copy.deepcopy(current);altered[key]='changed'
                with self.assertRaisesRegex(ValueError,'mismatch'):
                    restore_checkpoint(old,altered,rows)

    def test_duplicate_or_relabelled_checkpoint_row_rejects_resume(self):
        rows,current,old=setup_checkpoint()
        altered=copy.deepcopy(old);altered['outputs'].append(altered['outputs'][0])
        with self.assertRaisesRegex(ValueError,'identity'):restore_checkpoint(altered,current,rows)
        altered=copy.deepcopy(old);altered['outputs'][0]['title']='different title'
        with self.assertRaisesRegex(ValueError,'identity'):restore_checkpoint(altered,current,rows)

    def test_pure_rate_limit_does_not_split_extraction_into_more_calls(self):
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({},'', '')):
            client=SemanticHeadlineClient(['zhipu'],review=False,action_review=True)
        with patch.object(client,'_request',return_value=rate()) as ask:
            outputs=client.analyze_many([{'n':n,'title':f'headline {n}'} for n in range(1,11)])
        self.assertEqual(ask.call_count,1)
        self.assertTrue(batch_rate_limited(outputs))
        self.assertTrue(all(o['status']=='unavailable' for o in outputs))

    def test_pure_rate_limit_does_not_split_facts_into_more_calls(self):
        first=record({'results':[{'n':1,'analysis':lane()},{'n':2,'analysis':lane()}]})
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({},'', '')):
            client=SemanticHeadlineClient(['zhipu'],review=False,action_review=True)
        with patch.object(client,'_request',side_effect=[first,rate()]) as ask:
            outputs=client.analyze_many([{'n':1,'title':'FDA approves device'},{'n':2,'title':'FDA approves device again'}])
        self.assertEqual(ask.call_count,2)
        self.assertTrue(batch_rate_limited(outputs))
        self.assertEqual(outputs[0]['draft_analysis']['status'],'analyzed')

    def test_valid_first_stage_is_reused_without_a_new_lane_call(self):
        old=record(lane())
        second=record({'results':[{'n':7,'sectors':valid()['sectors'],'actions':valid()['actions'],'unknowns':[]}]})
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({},'', '')):
            client=SemanticHeadlineClient(['zhipu'],review=False,action_review=True)
        with patch.object(client,'_request',return_value=second) as ask:
            output=client.analyze_many([{'n':7,'title':'FDA approves device'}],resume_drafts={7:old})[0]
        self.assertEqual(ask.call_count,1)
        self.assertIs(output['model_output'],old['model_output'])
        self.assertTrue(output['analysis_output']['actions'][0]['concrete_public_action'])

    def test_resumed_draft_from_another_title_fails_before_request(self):
        old=record(lane())
        with patch('src.news_impact.semantic_headlines.route.load_keys',return_value=({},'', '')):
            client=SemanticHeadlineClient(['zhipu'],review=False,action_review=True)
        with patch.object(client,'_request') as ask:
            with self.assertRaisesRegex(ValueError,'evidence mismatch'):
                client.analyze_many([{'n':7,'title':'unrelated title'}],resume_drafts={7:old})
        ask.assert_not_called()

    def test_rate_limit_is_distinct_from_schema_failure_or_missing_keys(self):
        self.assertTrue(rate_limit_only(rate()))
        bad=rate();bad['rejected_outputs']=[{'validation_errors':['bad shape']}]
        self.assertFalse(rate_limit_only(bad))
        self.assertFalse(rate_limit_only({'status':'unavailable','attempts':[{'status':'credential_unavailable'}]}))

    def test_deadline_draft_map_supports_json_string_ids(self):
        rows=[{'n':7,'title':'FDA approves device'}];draft=record(lane())
        outputs=[{'n':7,'title':rows[0]['title'],'program_output':{'status':'unavailable','draft_analysis':{'7':draft}}}]
        self.assertEqual(resumable_drafts(rows,outputs),{7:draft})

    def test_atomic_checkpoint_write_preserves_existing_file_on_failure(self):
        with tempfile.TemporaryDirectory() as directory:
            path=pathlib.Path(directory)/'checkpoint.json'
            write_checkpoint(path,{'title':'初稿','outputs':[]})
            previous=path.read_bytes()
            with patch.object(pathlib.Path,'write_text',side_effect=OSError('interrupted')):
                with self.assertRaises(OSError):write_checkpoint(path,{'outputs':['replacement']})
            self.assertEqual(path.read_bytes(),previous)
            self.assertEqual(json.loads(previous)['title'],'初稿')

    def test_runner_rejects_changed_openclaw_transport_before_resume_calls(self):
        import contextlib,io,os,sys,types
        from run_semantic_headlines import main
        class Client:
            def __init__(self,*args,**kwargs):
                self.provider_order=['openclaw'];self.model_allowlist=('xai/grok-4.6',)
                self.review=False;self.sector_review=False;self.action_review=False;self.sector_roles=False
                self.catalog=types.SimpleNamespace(metadata={'available':True,'sha256':'catalog'})
        with tempfile.TemporaryDirectory() as directory:
            inp=pathlib.Path(directory)/'input.json';out=pathlib.Path(directory)/'output.json'
            inp.write_text('[]')
            argv=['runner','--input',str(inp),'--output',str(out),'--no-review','--resume']
            with patch.object(sys,'argv',argv),patch('run_semantic_headlines.SemanticHeadlineClient',Client),patch('run_semantic_headlines.signal.signal'),contextlib.redirect_stdout(io.StringIO()):
                with patch.dict(os.environ,{'OPENCLAW_LANE_TIMEOUT':'300','OPENCLAW_AGENT':'openclaw/default'}):main()
                saved=json.loads(out.read_text())
                self.assertEqual(saved['configuration']['openclaw_transport'],{'agent':'openclaw/default','timeout_seconds':300})
                with patch.dict(os.environ,{'OPENCLAW_LANE_TIMEOUT':'120','OPENCLAW_AGENT':'openclaw/default'}):
                    with self.assertRaisesRegex(ValueError,'configuration'):main()

    def test_runner_resumes_partial_batch_without_repeating_completed_row(self):
        import contextlib,io,sys,types
        from run_semantic_headlines import main
        calls=[]
        complete=record({});complete['analysis_output']={}
        failed=rate();failed['draft_analysis']=record(lane());failed['fact_review']=rate()
        class Client:
            def __init__(self,*args,**kwargs):
                self.provider_order=['zhipu'];self.model_allowlist=('glm-5.3-flash',)
                self.review=False;self.sector_review=True;self.action_review=True;self.sector_roles=True
                self.catalog=types.SimpleNamespace(metadata={'available':True,'sha256':'catalog'})
            def analyze_many(self,rows,resume_drafts=None):
                calls.append(([r['n'] for r in rows],resume_drafts))
                if len(calls)==1:return [complete,failed]
                return [complete]
        with tempfile.TemporaryDirectory() as directory:
            inp=pathlib.Path(directory)/'input.json';out=pathlib.Path(directory)/'output.json'
            inp.write_text(json.dumps([{'n':1,'title':'one'},{'n':2,'title':'FDA approves device'}]))
            argv=['runner','--input',str(inp),'--output',str(out),'--limit','2','--no-review','--action-review','--sector-roles','--resume']
            with patch.object(sys,'argv',argv),patch('run_semantic_headlines.SemanticHeadlineClient',Client),patch('run_semantic_headlines.signal.signal'),patch('run_semantic_headlines.signal.setitimer'),contextlib.redirect_stdout(io.StringIO()):
                main();first=json.loads(out.read_text());main();second=json.loads(out.read_text())
            self.assertEqual(first['stop_reason'],'rate_limited')
            self.assertEqual(second['generated_at'],first['generated_at'])
            self.assertEqual(second['analyzed'],2)
            self.assertEqual([r['n'] for r in second['outputs']],[1,2])
            self.assertEqual([ids for ids,_ in calls],[[1,2],[2]])
            self.assertEqual(set(calls[1][1]),{2})

if __name__=='__main__':unittest.main()
