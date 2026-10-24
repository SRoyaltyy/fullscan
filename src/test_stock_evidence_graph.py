import unittest
from .filing_relationships import load_parser,extract_sections
from .stock_evidence_graph import build


class StockEvidenceControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.nlp=load_parser()

    def graph(self,texts):
        return extract_sections([{'text':text,'source':'synthetic_control','accession':accession,
                'document':document,'filing_date':date,'kind':'business'}
                for text,accession,document,date in texts],ticker='TEST',name='Example Inc.',nlp=self.nlp)

    def one(self,text):return self.graph([(text,'a','annual.htm','2026-01-01')])['stock_evidence']

    def test_definition_in_another_section_supplies_distant_type(self):
        graph=self.graph([('Tinman processes applications.','a','annual.htm','2026-01-01'),
            ('We use our proprietary software platform ("Tinman").','a','annual.htm','2026-01-01')])
        result=graph['stock_evidence']
        rows=[o for o in result['occurrences'] if o['text']=='Tinman']
        self.assertGreaterEqual(len(rows),2)
        self.assertEqual(len({o['entity_id'] for o in rows}),1)
        entity=next(e for e in result['entities'] if e['id']==rows[0]['entity_id'])
        self.assertTrue(any(a['predicate']=='mentions_product_platform' and a['entity_id']==entity['id']
                            and a['basis']=='explicit_owned_definition' for a in result['assertions']))

    def test_full_name_joins_multiple_documents_with_all_witnesses(self):
        g=self.graph([('We partner with Acme Technologies Inc.','a','annual.htm','2026-01-01'),
                      ('We purchase batteries from Acme Technologies Inc.','b','update.htm','2026-02-01')])
        result=g['stock_evidence']; found=[e for e in result['entities'] if 'Acme Technologies' in e['name']]
        self.assertTrue(found)
        combined=[e for e in found if len({o['source_id'] for o in result['occurrences'] if o['entity_id']==e['id']})==2]
        self.assertTrue(combined)
        roles={a['predicate'] for a in result['assertions'] if a['entity_id']==combined[0]['id']}
        self.assertTrue({'partners_with','buys_from'}<=roles)

    def test_alias_cannot_borrow_another_documents_definition(self):
        g=self.graph([('Alpha Technologies ("AT") provides software.','a','annual.htm','2026-01-01'),
                      ('AT is discussed separately.','b','update.htm','2026-02-01')])
        rows=[o for o in g['stock_evidence']['occurrences'] if o['text']=='AT']
        self.assertFalse(any(o['definition_ids'] for o in rows if o['filing_date']=='2026-02-01'))

    def test_contradictory_definitions_are_not_silently_merged(self):
        result=self.one('Alpha Technologies ("AT") provides software. Beta Holdings ("AT") sells batteries. AT is discussed.')
        rows=[o for o in result['occurrences'] if o['text']=='AT']
        self.assertTrue(rows)
        self.assertTrue(all(o['identity_status']=='ambiguous_definition' and o['entity_id'] is None for o in rows))

    def test_negation_and_periods_survive_shared_identity(self):
        g=self.graph([('We do not partner with Acme Technologies.','a','annual.htm','2026-01-01'),
                      ('We may partner with Acme Technologies.','b','update.htm','2026-02-01')])
        assertions=g['stock_evidence']['assertions']
        rows=[a for a in assertions if a['predicate']=='partners_with']
        self.assertEqual({a['assertion'] for a in rows},{'negated','potential'})
        self.assertEqual({a['filing_date'] for a in rows},{'2026-01-01','2026-02-01'})
        self.assertTrue(all(a['effective_date'] is None for a in rows))

    def test_extraction_survives_missing_relationship(self):
        result=self.one('Acme Technologies Inc. shimmered yesterday.')
        self.assertTrue(any('Acme Technologies' in e['name'] for e in result['entities']))
        self.assertTrue(any(e['classification_status']=='unclassified' for e in result['entities']))

    def test_cutoff_excludes_future_definition(self):
        graph=self.graph([('Tinman is discussed.','a','annual.htm','2026-01-01'),
            ('Our software platform ("Tinman") processes applications.','b','update.htm','2026-02-01')])
        old=build(graph,as_of='2026-01-15')
        self.assertFalse(old['definitions'])
        self.assertTrue(all(o['filing_date']=='2026-01-01' for o in old['occurrences']))

    def test_legal_mentions_use_exact_disclosure_anchor_without_inventing_obligations(self):
        result=self.one('This document refers to Rule 405 of the Securities Act of 1933.')
        laws=[o for o in result['occurrences'] if 'explicit_disclosure' in o['extraction_basis']]
        self.assertTrue(any(o['text']=='Rule 405' for o in laws))
        self.assertTrue(any(o['text']=='Securities Act of 1933' for o in laws))
        self.assertTrue(any(a['predicate']=='mentions_legal_reference' and a['assertion']=='reported'
                            for a in result['assertions']))

    def test_nominal_agreement_binds_issuer_and_named_target_without_calling_it_closed(self):
        result=self.one('The Company entered into an agreement to acquire the equity interests of Apex Semiconductor Inc. ("Apex").')
        targets={e['id'] for e in result['entities'] if 'Apex Semiconductor' in e['name']}
        self.assertTrue(any(a['entity_id'] in targets and a['predicate']=='acquires' and a['assertion']=='potential'
                            for a in result['assertions']))

    def test_external_actor_agreement_is_not_an_issuer_acquisition(self):
        result=self.one('Our supplier entered into an agreement to acquire Apex Semiconductor Inc.')
        self.assertFalse(any(a['predicate']=='acquires' for a in result['assertions']))


if __name__=='__main__':unittest.main()
