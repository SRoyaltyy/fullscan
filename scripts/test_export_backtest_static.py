import json
import tempfile
import subprocess
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
from scripts import export_backtest_static as export


class ExportTests(unittest.TestCase):
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
