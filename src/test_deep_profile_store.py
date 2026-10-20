import gzip
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from .deep_profile_store import attach_profiles, latest_claims, read_index, read_profile
from .stock_evidence import Catalog, Profile, index_profiles
from .scanner_backtest import profile_view


class DeepProfileControls(unittest.TestCase):
    def graph(self):
        from .filing_relationships import VERSION, classify_paragraph
        text = 'We purchase hard drives from Apex Corporation.'
        source = {'id': 'annual', 'source': 'SEC', 'filing_date': '2026-01-01',
                  'accession': 'annual', 'section': 'Item 1', 'document': 'annual.htm',
                  'characters': len(text), 'sha256': hashlib.sha256(text.encode()).hexdigest()}
        fact = {'id': 'claim', 'subject': 'TEST', 'predicate': 'buys_from', 'object': 'Apex Corporation',
                'object_type': 'organization', 'status': 'asserted', 'confidence': 'syntax',
                'qualifiers': {'inputs': ['hard drives']},
                'evidence': [{'source_id': 'annual', 'start': 0, 'end': len(text), 'quote': text}]}
        paragraph = classify_paragraph(text, source_id='annual', start=0, end=len(text))
        paragraph.update(relationship_ids=['claim'], extraction_status='relationships_extracted')
        return {'schema': VERSION, 'ticker': 'TEST', 'tickers': ['TEST'],
                'sources': [source], 'facts': [fact], 'as_of': '2026-10-20', 'paragraphs': [paragraph],
                'coverage': {'facts': 1, 'paragraphs': 1, 'all_paragraphs_classified': True,
                             'source_sections': 1, 'characters_read': len(text),
                             'all_prose_dependency_analyzed': True, 'prose_characters_analyzed': len(text)}}

    def fixture(self, root, graph=None):
        graph = graph or self.graph()
        raw = gzip.compress(json.dumps(graph).encode(), mtime=0)
        (root/'issuers').mkdir()
        (root/'issuers/0000000001.json.gz').write_bytes(raw)
        entry = {'path': 'issuers/0000000001.json.gz', 'sha256': hashlib.sha256(raw).hexdigest(), 'cik': '0000000001'}
        index = {'schema': graph['schema'], 'complete': True, 'as_of': graph['as_of'],
                 'profiles': {'TEST': entry}, 'totals': {'companies': 1, 'filings': 1, 'documents': 1, 'characters': 43}}
        from .filing_relationships import compiler_fingerprint
        index['compiler_sha256'] = compiler_fingerprint()
        (root/'index.json').write_text(json.dumps(index))
        return entry

    def test_later_negative_claim_blocks_earlier_supplier_assertion(self):
        graph = self.graph()
        graph['sources'].append({**graph['sources'][0], 'id': 'quarterly', 'filing_date': '2026-09-01'})
        graph['facts'].append({**graph['facts'][0], 'id': 'negative', 'status': 'negated',
                               'evidence': [{'source_id': 'quarterly', 'quote': 'We do not purchase from Apex Corporation.', 'start': 0, 'end': 40}]})
        claims = latest_claims(graph, '2026-10-20')
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0][0]['status'], 'negated')
        self.assertEqual(latest_claims(graph, '2026-06-01')[0][0]['status'], 'asserted')

    def test_same_date_contradiction_is_not_promoted_to_current_supplier(self):
        graph = self.graph()
        graph['facts'].append({**graph['facts'][0], 'id': 'negative', 'status': 'negated'})
        self.assertEqual(latest_claims(graph, '2026-10-20')[0][0]['status'], 'negated')

    def test_graph_hash_and_path_are_verified(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            entry = self.fixture(root)
            self.assertEqual(read_profile(root, entry)['ticker'], 'TEST')
            with self.assertRaisesRegex(ValueError, 'checksum'):
                read_profile(root, {**entry, 'sha256': 'wrong'})
            with self.assertRaisesRegex(ValueError, 'escapes'):
                read_profile(root, {**entry, 'path': '../outside.json.gz'})

    def test_incomplete_set_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            data = json.loads((root/'index.json').read_text())
            data['complete'] = False
            (root/'index.json').write_text(json.dumps(data))
            with self.assertRaisesRegex(ValueError, 'incomplete'):
                read_index(root)

    def test_typed_dependency_reaches_same_board_and_preserves_full_witness(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            profiles = {'TEST': Profile('TEST', 'Example Industries', 'Hardware', 'Technology'),
                        'APEX': Profile('APEX', 'Apex Corporation', 'Components', 'Technology')}
            board, coverage = attach_profiles(index_profiles(profiles), root, as_of='2026-10-20',
                catalog=Catalog([{'ticker': 'APEX', 'name': 'Apex Corporation'}]))
            self.assertEqual(coverage['scope'], 'deep_filing_relationships')
            self.assertIn('APEX', board.dependencies)
            fact = board.profiles['TEST'].facts[0]
            self.assertEqual(fact.qualifiers['inputs'], ['hard drives'])
            self.assertEqual(fact.filing_date, '2026-01-01')
            self.assertEqual(profile_view(board.profiles['TEST'])['facts']['supplier'][0]['relationship_id'], 'claim')

    def test_profile_view_does_not_silently_cap_facts_or_passages(self):
        from .stock_evidence import Fact
        profile = Profile('TEST', 'Example', '', '')
        quote = 'x' * 5000
        profile.facts = [Fact('supplier', str(i), quote) for i in range(85)]
        result = profile_view(profile)
        self.assertEqual(len(result['facts']['supplier']), 85)
        self.assertEqual(result['facts']['supplier'][-1]['passage'], quote)


if __name__ == '__main__':
    unittest.main()
