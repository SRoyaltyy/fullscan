import importlib.util
import json
import tempfile
import unittest
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
            unavailable = {'analysis': {'reject_reason': 'lane_classify_missing'}}
            self.assertEqual(len(worker.pending(root, {worker.input_key(good): unavailable})), 1)

if __name__ == '__main__': unittest.main()
