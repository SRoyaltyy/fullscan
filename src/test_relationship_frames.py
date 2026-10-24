"""Capacity/scope controls. Synthetic controls are not unseen-stock evidence."""
import unittest
import json
from pathlib import Path
from .filing_relationships import load_parser, extract_doc
from .term_inventory import parse_terms


class FrameControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.nlp=load_parser()

    def claims(self, text):
        return extract_doc(self.nlp(text),ticker='TEST',name='Example Industries Inc.',source={'id':'test'})

    def test_benefits_are_retained_under_their_own_capacity(self):
        facts=self.claims('We offer retirement plans to employees.')
        self.assertTrue(any(f['predicate']=='provides_employee_benefit' for f in facts))
        self.assertFalse(any(f['predicate']=='offers' for f in facts))

    def test_a_pension_administration_service_can_be_a_commercial_product(self):
        facts=self.claims('We sell pension administration services to Acme.')
        self.assertTrue(any(f['predicate']=='offers' and 'services' in f['object'] for f in facts))
        self.assertTrue(any(f['predicate']=='sells_to' and 'Acme' in f['object'] for f in facts))

    def test_equity_option_is_not_a_physical_input(self):
        facts=self.claims('We purchase equity options.')
        self.assertTrue(any(f['predicate']=='holds_financial_instrument' for f in facts))
        self.assertFalse(any(f['predicate']=='uses_input' for f in facts))

    def test_real_operating_inputs_and_suppliers_survive(self):
        facts=self.claims('We purchase batteries from Acme.')
        self.assertTrue(any(f['predicate']=='uses_input' and f['object']=='batteries' for f in facts))
        self.assertTrue(any(f['predicate']=='buys_from' and f['object']=='Acme' for f in facts))

    def test_asset_sale_is_not_an_operating_customer(self):
        facts=self.claims('We agreed to sell our 70% interest to Acme.')
        f=next(f for f in facts if f['object']=='Acme')
        self.assertEqual(f['predicate'],'transaction_counterparty')
        self.assertEqual(f['status'],'potential')

    def test_financing_is_not_commercial_sales(self):
        facts=self.claims('We sell notes to Acme.')
        self.assertTrue(any(f['predicate']=='financing_counterparty' for f in facts))
        self.assertFalse(any(f['predicate']=='sells_to' for f in facts))

    def test_nested_belief_is_uncertainty_not_a_positive_obligation(self):
        facts=self.claims('We do not believe we are subject to regulation by FINRA.')
        f=next(f for f in facts if f['predicate']=='subject_to')
        self.assertEqual(f['status'],'uncertain')
        self.assertTrue(f['qualifiers']['assertion_scope']['belief_negated'])

    def test_negation_applies_to_its_own_coordinated_objects(self):
        facts=self.claims('We offer a trading tool and not a trading platform or exchange.')
        status={f['object']:f['status'] for f in facts if f['predicate']=='offers'}
        self.assertEqual(status['trading tool'],'asserted')
        self.assertEqual(status['trading platform'],'negated')
        self.assertEqual(status['exchange'],'negated')

    def test_another_clause_cannot_supply_historical_scope(self):
        facts=self.claims('We previously relied on Beta while we now purchase batteries from Acme.')
        self.assertTrue(any(f['object']=='Acme' and f['status']=='asserted' for f in facts))

    def test_paragraph_boundaries_prevent_heading_attachment(self):
        facts=self.claims('EXHIBIT 99.1\n\nANNOUNCEMENT\n\nWe purchase copper from Acme.')
        self.assertTrue(any(f['predicate']=='buys_from' and f['object']=='Acme' for f in facts))
        self.assertTrue(all('EXHIBIT' not in f['evidence']['quote'] for f in facts))

    def test_unsupported_relation_never_deletes_term_candidate(self):
        text='We sell to Acme.'
        facts=self.claims(text)
        self.assertTrue(any(f['predicate']=='unresolved_relationship' for f in facts))
        self.assertTrue(any(r['text']=='Acme' for r in parse_terms(self.nlp(text))['terms']))

    def test_unchanged_original_sec_witnesses_keep_terms_and_correct_capacity(self):
        path=Path(__file__).resolve().parents[1]/'eval/scanner-failures/original-capacity-reproductions.json'
        for case in json.loads(path.read_text()):
            with self.subTest(case=case['id']):
                doc=self.nlp(case['quote'])
                facts=extract_doc(doc,ticker=case['ticker'],name=case['name'],source={'id':'witness'})
                relevant=[f for f in facts if case['target'] in f['object']]
                self.assertTrue(any(f['predicate']==case['expected_predicate']
                                    and f['status']==case['expected_assertion'] for f in relevant),
                                [(f['predicate'],f['object'],f['status']) for f in facts])
                self.assertTrue(any(case['target'] in r['text'] for r in parse_terms(doc)['terms']))


if __name__=='__main__':unittest.main()
