import json,tempfile,unittest
from pathlib import Path
from .news_intake_google_resolver import resolve_google_url
from .news_intake_review_export import export_review_request

class ReviewExportTests(unittest.TestCase):
    def test_rpc_returns_publisher(self):
        rpc=json.dumps([['wrb.fr','Fbv4je',json.dumps(['garturlres','https://publisher.example/article'])]])
        url,requests=resolve_google_url('https://news.google.com/rss/articles/ABC',
            lambda url:(b'<div data-n-a-sg="sig" data-n-a-ts="123">','text/html'),
            lambda url,form:")]}'\n\n"+rpc)
        self.assertEqual(url,'https://publisher.example/article');self.assertEqual(len(requests),2)
    def test_missing_parameters_abstains(self):
        with self.assertRaises(ValueError):resolve_google_url('https://news.google.com/articles/ABC',lambda u:(b'consent','text/html'))
    def test_non_google_passes_without_network(self):
        self.assertEqual(resolve_google_url('https://publisher.example/a',lambda u:self.fail())[0],'https://publisher.example/a')
    def test_failure_is_saved_and_not_retried_in_same_window(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'config').mkdir();archive=root/'data/news_intake/2026-10-04';archive.mkdir(parents=True)
            request={'session_id':'test','resolve_full_text':True,'fulltext_budget':8,
                     'cases':[{'archive_date':'2026-10-04','document_id':'abc'}]}
            (root/'config/news_review_request.json').write_text(json.dumps(request))
            doc={'id':'abc','title':'Headline','body':'Headline','url':'https://publisher.example/a','extraction_status':'feed_text'}
            (archive/'documents.json').write_text(json.dumps([doc]))
            calls=[]
            def fail(url):calls.append(url);raise ValueError('Offline fixture')
            export_review_request(root,fail,lambda text:text);export_review_request(root,fail,lambda text:text)
            self.assertEqual(len(calls),1)
            saved=json.loads((root/'data/news_intake/review_exports/test/abc.json').read_text())
            self.assertEqual(saved['body'],doc['body']);self.assertEqual(saved['review_enrichment']['attempts'],1)
    def test_traversal_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'config').mkdir()
            (root/'config/news_review_request.json').write_text(json.dumps({'session_id':'../bad','cases':[]}))
            with self.assertRaises(ValueError):export_review_request(root)

if __name__=='__main__':unittest.main()
