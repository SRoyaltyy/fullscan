import json
import tempfile
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from scripts import export_backtest_static as export


class ExportTests(unittest.TestCase):
    def test_cached_article_parse_is_complete_and_bound_to_the_scored_headline(self):
        import hashlib
        value='DraftKings faces regulations'
        parsed={'input_sha256':hashlib.sha256(value.encode()).hexdigest(),'parser':{'model':'pinned'},
            'mentions':[{'start':0,'end':10,'text':'DraftKings','label':'ORG'}],
            'noun_phrases':[{'start':0,'end':10,'text':'DraftKings','head':0}],
            'syntax':[{'start':0,'end':10,'text':'DraftKings','lemma':'DraftKings','pos':'PROPN','dependency':'nsubj','head':1},
                      {'start':11,'end':16,'text':'faces','lemma':'face','pos':'VERB','dependency':'ROOT','head':1},
                      {'start':17,'end':28,'text':'regulations','lemma':'regulation','pos':'NOUN','dependency':'dobj','head':1}]}
        article={'id':'one','text':value,'input_mode':'headline','entity_annotations':parsed}
        result=export.export_articles([article],5)[0]
        self.assertEqual(result['text'],value)
        self.assertFalse(result['truncated'])
        self.assertEqual(len(result['article_parse']['syntax']),3)
        self.assertEqual(result['article_parse']['input_sha256'],parsed['input_sha256'])
        parsed['input_sha256']='wrong'
        with self.assertRaisesRegex(ValueError,'scored text'):export.export_articles([article],5)

    def test_compact_export_retains_all_keywords_and_evidence_without_raw_graph_copy(self):
        import gzip,hashlib
        from types import SimpleNamespace
        from src.test_deep_profile_store import DeepProfileControls
        from src.stock_evidence import Profile,Catalog,index_profiles
        from src.stock_keyword_store import attach_keyword_profiles
        from scripts.publish_scanner_snapshot import validate
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);source=root/'source';source.mkdir();out=root/'out';out.mkdir()
            entry=DeepProfileControls().fixture(source)
            # Exercise the complete source -> keywords -> compressed assets ->
            # publication gate with a disclosure that has no old relationship.
            graph_path=source/entry['path']
            graph=json.loads(gzip.decompress(graph_path.read_bytes()))
            text='\nWe comply with the Alpha Privacy Act.'
            from src.filing_relationships import classify_paragraph
            start=graph['sources'][0]['characters']
            graph['paragraphs'].append(classify_paragraph(text,source_id=graph['sources'][0]['id'],start=start,end=start+len(text)))
            combined=''.join(p['text'] for p in graph['paragraphs'])
            graph['sources'][0].update(characters=len(combined),sha256=hashlib.sha256(combined.encode()).hexdigest())
            graph['coverage'].update(paragraphs=2,characters_read=len(combined),prose_characters_analyzed=len(combined))
            from src.specific_filing_records import details
            graph['specific_records']=details(graph)
            packed=gzip.compress(json.dumps(graph).encode());graph_path.write_bytes(packed)
            index=json.loads((source/'index.json').read_text());index['profiles']['TEST']['facts']=1
            index['profiles']['TEST']['sha256']=hashlib.sha256(packed).hexdigest()
            (source/'index.json').write_text(json.dumps(index))
            board,coverage=attach_keyword_profiles(index_profiles({'TEST':Profile('TEST','Example','','')}),source,
                as_of='2026-10-20',catalog=Catalog([]))
            original=dict(board.profiles['TEST'].deep_profile)
            profiles,keyword_index=export.export_profiles(SimpleNamespace(evidence_board=board),out,
                filing_profiles=source,profile_assets='keywords',archive_url='https://github.com/SRoyaltyy/fullscan/actions/runs/1')
            payload=json.loads(gzip.decompress((out/profiles['TEST']['profile_path']).read_bytes()))
            self.assertEqual(payload['keyword_coverage']['retained_relationships'],1)
            self.assertEqual(payload['keyword_coverage']['specific_disclosure_records'],1)
            self.assertEqual(len(payload['keyword_evidence']),2)
            self.assertFalse((out/'filing-profiles/issuers').exists())
            self.assertNotIn('path',payload['deep_profile'])
            self.assertEqual(board.profiles['TEST'].deep_profile,original)
            snapshot={'snapshot_id':'snapshot','source_revision':'source','input_mode':'headline',
                'filing_coverage':coverage,'profile_publication':{'mode':'keywords'}}
            for name,payload in {'profiles.json':profiles,'articles.json':[{'id':'one'}],
                    'ticker-index.json':{},'meta.json':{'articles':1,'snapshot':snapshot,'keyword_index':keyword_index},
                    'results.jsonl':{'id':'one','snapshot_id':'snapshot'}}.items():
                (out/name).write_text(json.dumps(payload))
            validate(out,'source')
            receipt=json.loads((out/'filing-profiles/publication.json').read_text())
            receipt['profiles']['TEST']['retained_relationships']=0
            (out/'filing-profiles/publication.json').write_text(json.dumps(receipt))
            with self.assertRaisesRegex(ValueError,'receipt'):
                validate(out,'source')

    def test_isolated_export_multiprocessing_and_resume_end_to_end(self):
        from src.link_correlation import ROOT
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            catalog = root / 'descriptions.csv'
            catalog.write_text('Ticker,Company,Industry,Sector,Description\n'
                'AAPL,Apple Inc,Consumer Electronics,Technology,Apple manufactures smartphones and wearable computing devices.\n')
            news = root / 'news.json'
            news.write_text(json.dumps([{'id': 'one', 'title': 'Apple shares rise', 'body': 'unused body'}]))
            command = [sys.executable, '-I', '-S', str(ROOT/'scripts/offline_evidence_checks.py'),
                       '--backtest-export', '--news', str(news), '--descriptions', str(catalog),
                       '--workers', '2', '--out-dir', str(root/'out')]
            first = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=90)
            self.assertEqual(first.returncode, 0, first.stderr)
            second = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=90)
            self.assertEqual(second.returncode, 0, second.stderr)
            rows = [json.loads(line) for line in (root/'out/results.jsonl').read_text().splitlines()]
            self.assertEqual(len(rows), 1)
            self.assertEqual({p['ticker'] for p in rows[0]['passes']}, {'AAPL'})
            meta = json.loads((root/'out/meta.json').read_text())
            self.assertEqual(rows[0]['snapshot_id'], meta['snapshot']['snapshot_id'])
            rejected = subprocess.run(command + ['--input-mode', 'full-text'], cwd=ROOT,
                                      capture_output=True, text=True, timeout=90)
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn('snapshot mismatch', rejected.stderr)

    def test_resume_rejects_historical_unversioned_cache(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'results.jsonl').write_text('{}\n')
            with self.assertRaisesRegex(ValueError, 'unversioned'):
                export.validate_resume(root, {'snapshot_id': 'new'})

    def test_resume_rejects_different_engine_or_input_fingerprint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'meta.json').write_text(json.dumps({'snapshot': {'snapshot_id': 'old'}}))
            with self.assertRaisesRegex(ValueError, 'snapshot mismatch'):
                export.validate_resume(root, {'snapshot_id': 'new'})

    def test_resume_checks_every_record_and_duplicate_id(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'meta.json').write_text(json.dumps({'snapshot': {'snapshot_id': 'same'}}))
            result = {'id': 'x', 'snapshot_id': 'same'}
            path = root / 'results.jsonl'
            path.write_text(json.dumps(result) + '\n')
            self.assertEqual(export.validate_resume(root, {'snapshot_id': 'same'}), [result])
            path.write_text(json.dumps(result) + '\n' + json.dumps(result) + '\n')
            with self.assertRaisesRegex(ValueError, 'duplicate'):
                export.validate_resume(root, {'snapshot_id': 'same'})
            path.write_text(json.dumps({**result, 'snapshot_id': 'other'}) + '\n')
            with self.assertRaisesRegex(ValueError, 'incompatible'):
                export.validate_resume(root, {'snapshot_id': 'same'})

    def test_export_default_passes_only_the_headline_to_shared_engine(self):
        seen = []
        def fake(board, articles, **kwargs):
            seen.extend(articles)
            self.assertEqual(kwargs['engine'], 'evidence')
            return [{'id': a['id'], 'stocks': []} for a in articles]
        with patch.object(export, 'link_articles', fake), \
             patch.object(export, '_SNAPSHOT', {'snapshot_id': 'test'}):
            result = export._score_shard([{'id': 'x', 'title': 'headline', 'text': 'body only'}])
        self.assertEqual(seen[0]['text'], 'headline')
        self.assertEqual(result[0]['snapshot_id'], 'test')
        self.assertEqual(result[0]['input_mode'], 'headline')

    def test_empty_engine_shard_cannot_be_silently_zipped_away(self):
        with patch.object(export, 'link_articles', return_value=[]):
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                export._score_shard([{'id': 'x', 'title': 'headline'}])

    def test_profile_enricher_cannot_overwrite_scoring_snapshot(self):
        from scripts import export_ticker_profiles as legacy
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'meta.json').write_text(json.dumps({'snapshot': {'snapshot_id': 'test'}}))
            with patch.object(legacy, 'OUT_DIR', root), patch.object(legacy, 'edgar_get') as fetch:
                with self.assertRaises(SystemExit):
                    legacy.main([])
                fetch.assert_not_called()


if __name__ == '__main__':
    unittest.main()

