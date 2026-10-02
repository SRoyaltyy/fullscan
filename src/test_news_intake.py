"""Tests for silent loss, false completeness and reproducible free parsing."""
import tempfile
import unittest
import json
from unittest.mock import patch
from datetime import datetime, timezone
from pathlib import Path

from .news_intake import collect_source, feed_rows, first_pass, merge_documents, registry, run
from .news_intake_compare import compare
from .news_search_catalog import validate

NOW = datetime(2026, 10, 2, 9, 0, tzinfo=timezone.utc)


def rss(n=1, dated=True):
    date = '<pubDate>Fri, 02 Oct 2026 08:00:00 GMT</pubDate>' if dated else ''
    return ('<rss><channel>' + ''.join(f'<item><title>Company {i} acquisition</title><link>https://example.org/{i}</link>{date}</item>' for i in range(n)) + '</channel></rss>').encode()


class IntakeTests(unittest.TestCase):
    def test_locked_enum_and_unique_source_ids(self):
        validate()
        specs = registry()
        self.assertEqual(len(specs), len({s['id'] for s in specs}))
        self.assertGreater(len(specs), 90)

    def test_no_feed_starvation_and_saturation_visible(self):
        for source in ['first', 'last']:
            rows, status = collect_source({'id': source, 'kind': 'feed', 'url': 'https://test'}, NOW.replace(hour=0), NOW, lambda _: (rss(150), 'application/rss+xml'))
            self.assertEqual(len(rows), 150)
            self.assertEqual(status['status'], 'ok')
        _, status = collect_source({'id': 'search', 'kind': 'search', 'query': 'acquisition'}, NOW.replace(hour=0), NOW, lambda _: (rss(100), 'application/rss+xml'))
        self.assertEqual(status['status'], 'partial_saturated')

    def test_bad_source_not_empty_success(self):
        rows, health = collect_source({'id': 'bad', 'kind': 'feed', 'url': 'x'}, NOW, NOW, lambda _: (b'<html>denied</html>', 'text/html'))
        self.assertEqual(rows, [])
        self.assertEqual(health['status'], 'failed')

    def test_dates_atom_and_unknown_publication(self):
        row = feed_rows(rss(dated=False))[0]
        self.assertEqual(row['published_at'], '')
        atom = b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><title>Filing</title><link href="https://sec.gov/test"/><updated>2026-10-02T08:00:00Z</updated></entry></feed>'
        self.assertEqual(feed_rows(atom)[0]['url'], 'https://sec.gov/test')

    def test_replay_preserves_evidence_and_observations(self):
        a = {'title': 'Company acquisition', 'url': 'https://example.org/a?utm_source=x', 'source': 'first', 'body': 'long evidence', 'published_at': NOW.isoformat()}
        initial = merge_documents([], [a], '2026-10-02T08:00:00+00:00')
        second = merge_documents(initial, [a | {'source': 'second', 'body': 'x', 'url': 'https://example.org/a'}], '2026-10-02T09:00:00+00:00')
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]['first_seen'], '2026-10-02T08:00:00+00:00')
        self.assertEqual(second[0]['body'], 'long evidence')
        self.assertEqual(len(second[0]['observations']), 2)

    def test_frame_does_not_erase_deal_and_board_is_candidate(self):
        for title, expected in [('CoreWeave lands deal: how to play the stock', 'demand'), ('Donald Trump Jr joins the board of Company', 'key_person')]:
            doc = merge_documents([], [{'title': title, 'url': '', 'published_at': NOW.isoformat(), 'source': 'test'}], NOW.isoformat())[0]
            parsed = first_pass(doc, NOW)
            self.assertIn(expected, parsed['candidate_classes'])
            self.assertEqual(parsed['state'], 'candidate')
            self.assertIsNone(parsed['final_lane_class'])
            self.assertEqual(parsed['routing_class'], expected)
            self.assertEqual(parsed['analyst_family'], 'firm' if expected == 'key_person' else 'quantity')

    def test_unknown_and_rumor_are_review_never_silent_drops(self):
        for title in ['Entirely unfamiliar development', 'Company reportedly in talks for acquisition']:
            doc = merge_documents([], [{'title': title, 'url': '', 'source': 'test'}], NOW.isoformat())[0]
            parsed = first_pass(doc, NOW)
            self.assertEqual(parsed['state'], 'review')

    def test_fallback_keeps_original_failure_visible(self):
        def fetch(url):
            if url == 'broken':
                raise ValueError('broken feed')
            return rss(), 'application/rss+xml'
        rows, health = collect_source({'id': 'rss_ap_business', 'kind': 'feed', 'url': 'broken'}, NOW.replace(hour=0), NOW, fetch)
        self.assertEqual(health['status'], 'fallback_search')
        self.assertIn('broken feed', health['error'])
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]['primary'])

    def test_reparse_has_no_network_and_preserves_full_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = root / 'data/news_intake/2026-10-02/documents.json'
            ledger.parent.mkdir(parents=True)
            docs = merge_documents([], [{'title':'Company acquisition', 'url':'https://example.org/a', 'body':'Evidence '*1000, 'published_at': NOW.isoformat(), 'source':'fixture'}], NOW.isoformat())
            ledger.write_text(json.dumps(docs))
            with patch('src.news_intake.get', side_effect=AssertionError('network forbidden')):
                report = run(root, '2026-10-02', now=NOW, specs=[], parse_only=True)
            self.assertEqual(report['document_count'], 1)
            self.assertEqual(json.loads(ledger.read_text())[0]['body'], 'Evidence '*1000)
            self.assertEqual(report['paid_api_calls'], 0)

    def test_hosted_sec_block_uses_index_without_claiming_primary(self):
        def fetch(url):
            if 'sec.gov' in url:
                raise ValueError('HTTP 403 from runner')
            return rss(), 'application/rss+xml'
        rows, health = collect_source({'id':'sec_8-K', 'kind':'sec', 'form':'8-K', 'primary':True, 'quick':True}, NOW.replace(hour=0), NOW, fetch)
        self.assertEqual(health['status'], 'fallback_search')
        self.assertIn('403', health['error'])
        self.assertEqual(len(rows), 1)
        self.assertFalse(rows[0]['primary'])

    def test_comparison_does_not_mislabel_keyword_match_as_verified_recall(self):
        docs = merge_documents([], [{'title':'Company X FDA approval', 'url':'', 'source':'fixture'}], NOW.isoformat())
        report = compare(docs, [{'event_id':'a', 'keywords':'Company X;FDA approval'}, {'event_id':'b','keywords':'Company Y;acquisition'}])
        self.assertEqual(report['possible_matches'], 1)
        self.assertIsNone(report['verified_recall'])
        self.assertEqual(report['results'][1]['status'], 'not_found')


if __name__ == '__main__':
    unittest.main()
