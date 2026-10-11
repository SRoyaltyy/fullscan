import importlib.util
import json
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

spec = importlib.util.spec_from_file_location('lane_public_articles', Path(__file__).with_name('lane_public_articles.py'))
worker = importlib.util.module_from_spec(spec); spec.loader.exec_module(worker)

class PublicInputs(unittest.TestCase):
    def test_private_context_is_excluded_from_payload_and_hash(self):
        doc = {'id': 'one', 'title': 'Synthetic public news', 'body': 'Public body', 'url': 'https://example.com'}
        private = {**doc, 'context_documents': [{'text': 'Private context'}]}
        self.assertEqual(worker.input_key(doc), worker.input_key(private))
        self.assertNotIn('context_documents', worker.public_input(private))

    def test_pending_excludes_thin_and_completed_inputs(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); day = worker.datetime.now(worker.timezone.utc).date().isoformat()
            path = root / 'data/news_intake' / day / 'documents.json'; path.parent.mkdir(parents=True)
            good = {'id': 'one', 'title': 'Synthetic public news', 'body': 'Public text ' * 100, 'extraction_status': 'page_text'}
            path.write_text(json.dumps([good, {**good, 'id': 'thin', 'extraction_status': 'feed_text'}]))
            self.assertEqual(len(worker.pending(root, {})), 1)
            self.assertEqual(worker.pending(root, {worker.input_key(good): {}}), [])
            for reason in ('lane_classify_missing', 'lane_meta_missing', 'lane_filter_missing'):
                unavailable = {'analysis': {'reject_reason': reason}}
                self.assertEqual(len(worker.pending(root, {worker.input_key(good): unavailable})), 1)
            failed = {'status': 'execution_failed:ModuleNotFoundError', 'analysis': None}
            self.assertEqual(len(worker.pending(root, {worker.input_key(good): failed})), 1)

    def test_rejected_json_is_retained_without_accepting_it(self):
        rows = []
        check = worker.audit_acceptance('meta', lambda blob: bool(blob.get('valid')), rows)
        rejected = {'m1': {'need_context': 'no'}, 'm2': []}
        self.assertFalse(check(rejected))
        self.assertEqual(rows, [{'stage': 'meta', 'parsed': rejected}])
        self.assertTrue(check({'valid': True}))
        self.assertEqual(len(rows), 1)
        for _ in range(20): check(rejected)
        self.assertEqual(len(rows), 12)
        self.assertIsNone(worker.audit_acceptance('classify', None, rows))

    def test_quota_attempt_preserves_prior_analysis_and_producing_identity(self):
        previous = {'status': 'executed', 'analysis': {'event_class': 'access_control'},
                    'completed_at': '2026-10-10T00:00:00Z', 'producer_source': {'sha256': 'old'}, 'attempts': []}
        blocked = {'status': 'blocked_free_daily_quota', 'analysis': None,
                   'completed_at': '2026-10-11T00:00:00Z', 'producer_source': {'sha256': 'new'}}
        row = worker.retain_attempt(previous, blocked)
        self.assertEqual(row['analysis'], previous['analysis'])
        self.assertEqual(row['completed_at'], previous['completed_at'])
        self.assertEqual(row['producer_source']['sha256'], 'old')
        self.assertEqual(row['latest_attempt']['status'], 'blocked_free_daily_quota')
        self.assertEqual(row['attempts'][-1]['producer_source']['sha256'], 'new')
        self.assertEqual(previous['attempts'], [])

    def test_stale_producing_code_is_pending_even_for_a_successful_result(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder); day = worker.datetime.now(worker.timezone.utc).date().isoformat()
            path = root / 'data/news_intake' / day / 'documents.json'; path.parent.mkdir(parents=True)
            doc = {'id': 'one', 'body': 'Public reporting ' * 100, 'extraction_status': 'page_text'}
            path.write_text(json.dumps([doc]))
            old = {worker.input_key(doc): {'status': 'executed', 'analysis': {'keep': True}, 'producer_source': {'sha256': 'old'}}}
            self.assertEqual(len(worker.pending(root, old, source_sha256='new')), 1)
            self.assertEqual(worker.pending(root, old, source_sha256='old'), [])
            old[worker.input_key(doc)]['status'] = 'blocked_free_daily_quota'
            self.assertEqual(len(worker.pending(root, old, source_sha256='old')), 1)

    def test_daily_quota_stops_batch_and_reports_actual_attempts_without_calls(self):
        from src import lane_route
        capped_before = lane_route._OR_DAY_CAPPED
        class FakeLive:
            def __init__(self):
                self.ctx = {'keys': {'openrouter': 'synthetic-key'}}
            def __call__(self, *args, **kwargs):
                lane_route._OR_DAY_CAPPED = True
                return None, '', ''
        class FakeIndex:
            rows = [{}]; source = ''; title_names = set()
        def process(article, audited, **kwargs):
            audited('classify', 'Synthetic public prompt', 'Synthetic system')
            return {'keep': False, 'reject_reason': 'lane_classify_missing', 'prompt_log': []}
        try:
            lane_route._OR_DAY_CAPPED = False
            with tempfile.TemporaryDirectory() as folder:
                root = Path(folder); day = worker.datetime.now(worker.timezone.utc).date().isoformat()
                path = root / 'data/news_intake' / day / 'documents.json'; path.parent.mkdir(parents=True)
                docs = [{'id': str(i), 'body': 'Public reporting ' * 100, 'extraction_status': 'page_text'} for i in range(3)]
                path.write_text(json.dumps(docs))
                with patch('src.lane_one_shot.LiveLane', FakeLive), \
                     patch('src.news_impact.one_shot_stack.process_article', side_effect=process) as analyze, \
                     patch('src.news_impact.finviz_linker.get_index', return_value=FakeIndex()), \
                     patch('src.lane_source_identity.source_identity', return_value={'sha256': 'synthetic-source'}):
                    state = worker.run(root, root / 'outputs.json')
                self.assertEqual(analyze.call_count, 1)
                self.assertEqual(state['runtime_status'], 'free_model_daily_quota_exhausted')
                self.assertEqual(state['last_run_processed'], 1)
                self.assertEqual(state['last_run_model_stage_responses'], 0)
                self.assertEqual(len(state['records']), 1)
                self.assertEqual(next(iter(state['records'].values()))['status'], 'blocked_free_daily_quota')
        finally:
            lane_route._OR_DAY_CAPPED = capped_before

if __name__ == '__main__': unittest.main()
