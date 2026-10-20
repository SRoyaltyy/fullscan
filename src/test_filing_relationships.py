import hashlib
import json
import socket
import unittest
from unittest.mock import patch

from .filing_relationships import extract_sections, load_parser, windows


class FullTextControls(unittest.TestCase):
    def test_windows_cover_whole_document_without_loss_or_overlap(self):
        raw = ('A lengthy paragraph.\n\n'+('unusual text ' * 2000)) * 3
        chunks = list(windows(raw, 1000))
        self.assertEqual(''.join(text for _, text in chunks), raw)
        for start, text in chunks:
            self.assertEqual(raw[start:start+len(text)], text)


class RelationshipControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nlp = load_parser()

    def parse(self, text, kind='business'):
        # The parser must work with all socket connections disabled.
        with patch.object(socket, 'create_connection', side_effect=AssertionError('network')), \
             patch.object(socket.socket, 'connect', side_effect=AssertionError('network')):
            return extract_sections([{'text': text, 'kind': kind, 'source': 'synthetic',
                'filing_date': '2026-10-19', 'accession': '0000000001-26-000001',
                'document': 'annual.htm', 'section': 'Item 1'}],
                ticker='TEST', name='Example Industries Inc.', nlp=self.nlp)

    def claims(self, result, predicate):
        return [f for f in result['facts'] if f['predicate'] == predicate]

    def test_supplier_and_inputs_are_connected_in_same_clause(self):
        result = self.parse('We purchase hard drives and semiconductors from Toshiba Corporation.')
        suppliers = self.claims(result, 'buys_from')
        self.assertTrue(any('Toshiba' in f['object'] for f in suppliers), result)
        inputs = {f['object'] for f in self.claims(result, 'uses_input')}
        self.assertIn('hard drives', inputs)
        self.assertIn('semiconductors', inputs)
        self.assertTrue(any('hard drives' in f['qualifiers']['inputs'] for f in suppliers))

    def test_same_company_can_be_supplier_and_competitor_in_different_clauses(self):
        result = self.parse('We purchase processors from Nvidia. We compete with Nvidia in cloud services.')
        self.assertTrue(any('Nvidia' in f['object'] for f in self.claims(result, 'buys_from')))
        self.assertTrue(any('Nvidia' in f['object'] for f in self.claims(result, 'competes_with')))

    def test_other_company_purchase_does_not_become_issuer_dependency(self):
        result = self.parse('Apex Corporation purchases processors from Nvidia. We sell backup software.')
        self.assertFalse(self.claims(result, 'buys_from'), result)

    def test_our_customers_are_not_the_issuer(self):
        result = self.parse('Our customers purchase processors from Nvidia. We offer accounting software.')
        self.assertFalse(self.claims(result, 'buys_from'), result)

    def test_passive_supplier_and_explicit_supplier_list(self):
        result = self.parse('Our hard drives are supplied by Toshiba. Our suppliers include Western Digital and Toshiba.')
        suppliers = self.claims(result, 'buys_from')
        self.assertTrue(any('Toshiba' in f['object'] for f in suppliers), result)
        self.assertTrue(any('Western Digital' in f['object'] for f in suppliers), result)

    def test_long_dependency_phrase_is_retained(self):
        result = self.parse('We rely on geographically distributed third-party data center colocation facilities.')
        self.assertTrue(any('colocation facilities' in f['object'] for f in self.claims(result, 'depends_on')), result)

    def test_known_company_mention_is_not_globally_a_competitor(self):
        result = self.parse('We purchase equipment from Amazon Web Services.')
        self.assertFalse(self.claims(result, 'competes_with'), result)

    def test_negated_and_historical_and_potential_relationships_are_labelled(self):
        for text, status in [('We do not rely on Toshiba.', 'negated'),
                             ('We previously relied on Toshiba.', 'historical'),
                             ('We may rely on Toshiba.', 'potential')]:
            facts = self.claims(self.parse(text), 'depends_on')
            self.assertTrue(facts, text)
            self.assertEqual({f['status'] for f in facts}, {status})

    def test_agency_and_law_remain_different_types(self):
        result = self.parse('We are regulated by the FDA. We are subject to GDPR.')
        self.assertTrue(any(f['object_type'] == 'agency' for f in result['facts']), result)
        self.assertTrue(any(f['object_type'] == 'law' and f['object'] == 'GDPR' for f in result['facts']), result)

    def test_every_fact_has_exact_quote_and_offsets_and_dated_source(self):
        text = 'We purchase corn syrup from Maple Foods. We compete with Acme Corporation.'
        result = self.parse(text)
        sources = {s['id']: s for s in result['sources']}
        self.assertTrue(result['facts'])
        for fact in result['facts']:
            for ev in fact['evidence']:
                self.assertEqual(text[ev['start']:ev['end']], ev['quote'])
                self.assertEqual(sources[ev['source_id']]['filing_date'], '2026-10-19')
                self.assertEqual(sources[ev['source_id']]['sha256'], hashlib.sha256(text.encode()).hexdigest())

    def test_repeated_runs_produce_identical_serialized_output(self):
        text = 'We manufacture aircraft components. We purchase steel from Apex Corporation.'
        self.assertEqual(json.dumps(self.parse(text), sort_keys=True), json.dumps(self.parse(text), sort_keys=True))

    def test_risk_mention_does_not_invent_specific_supplier_or_country(self):
        result = self.parse('Tariffs could adversely affect our costs.', kind='risk')
        self.assertTrue(self.claims(result, 'risk_mentions'))
        self.assertFalse(self.claims(result, 'buys_from'))
        self.assertFalse(self.claims(result, 'operates_in'))


if __name__ == '__main__':
    unittest.main()
