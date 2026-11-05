"""Sealed scanner rows stay put when a later day is appended."""
import json
import tempfile
import unittest
from pathlib import Path

from scripts.scanner_fill_forward import (
    assert_sealed_prefix, prepare_forward_news,
)


class FillForwardTests(unittest.TestCase):
    def test_prefix_refuses_an_edited_sealed_line(self):
        assert_sealed_prefix('{"id":"a"}\n', '{"id":"a"}\n{"id":"b"}\n')
        with self.assertRaisesRegex(ValueError, 'sealed result'):
            assert_sealed_prefix('{"id":"a"}\n', '{"id":"CHANGED"}\n{"id":"b"}\n')

    def test_prepare_keeps_base_docs_and_skips_scored_ids(self):
        base = [{'id': 'old', 'published_at': '2026-10-17T12:00:00Z', 'title': 'sealed'}]
        later = [
            ('2026-10-26', [
                {'id': 'old', 'published_at': '2026-10-26T12:00:00Z', 'title': 'rewrite'},
                {'id': 'new', 'published_at': '2026-10-26T12:00:00Z', 'title': 'forward'},
                {'id': 'early', 'published_at': '2026-10-16T12:00:00Z', 'title': 'before'},
                {'id': 'future', 'published_at': '2026-11-06T12:00:00Z', 'title': 'after'},
            ]),
        ]
        out = prepare_forward_news(base, later, {'old'}, after='2026-10-17', through='2026-11-05')
        self.assertEqual(out[0], base[0])
        self.assertEqual([row['id'] for row in out], ['old', 'new'])
        from scripts.scanner_fill_forward import earliest_published_day
        two = out + [{'id': 'later', 'published_at': '2026-10-27T12:00:00Z', 'title': 'next'}]
        one = earliest_published_day(two, 1)
        self.assertEqual([row['id'] for row in one], ['old', 'new'])

    def test_prepare_file_roundtrip(self):
        from scripts.scanner_fill_forward import prepare_file
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            base = root / '2026-10-17' / 'documents.json'
            base.parent.mkdir()
            base.write_text(json.dumps([{'id': 'old', 'published_at': '2026-10-17'}]), encoding='utf-8')
            day = root / '2026-11-05' / 'documents.json'
            day.parent.mkdir()
            day.write_text(json.dumps([
                {'id': 'old', 'published_at': '2026-11-05'},
                {'id': 'n', 'published_at': '2026-11-05'},
            ]), encoding='utf-8')
            sealed = root / 'results.jsonl'
            sealed.write_text('{"id":"old","snapshot_id":"s"}\n', encoding='utf-8')
            out = root / 'news.json'
            added = prepare_file(base, root, sealed, after='2026-10-17', through='2026-11-05', out=out)
            docs = json.loads(out.read_text(encoding='utf-8'))
            self.assertEqual(added, 1)
            self.assertEqual([d['id'] for d in docs], ['old', 'n'])
            self.assertEqual(docs[0]['published_at'], '2026-10-17')


if __name__ == '__main__':
    unittest.main()
