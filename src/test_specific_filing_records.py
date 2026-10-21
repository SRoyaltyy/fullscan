import hashlib
import json
from pathlib import Path
import unittest
from .specific_filing_records import details,validate_details,specificity


def graph(text,kind='risk'):
    return {'ticker':'TEST','name':'Example Industries Inc.','sources':[{'id':'source','kind':kind}],
            'paragraphs':[{'id':'paragraph','source_id':'source','start':17,'end':17+len(text),
                'text':text,'kind':'prose','sha256':hashlib.sha256(text.encode()).hexdigest(),'labels':['regulation_legal']}],
            'facts':[]}


class SpecificDisclosureControls(unittest.TestCase):
    def parse(self,text,kind='risk'):
        g=graph(text,kind);d=details(g);validate_details(g,d);return d['records']

    def test_nested_identifiers_and_parenthetical_aliases_preserve_one_law_identity(self):
        rows=self.parse('We are subject to Regulation (EU) 2016/679 (also known as the General Data Protection Regulation or “GDPR”) and the California Privacy Rights Act (“CPRA”).')
        laws=[r for r in rows if r['category']=='law']
        self.assertEqual(len(laws),2)
        gdpr=next(r for r in laws if r['entity'].startswith('Regulation'))
        self.assertEqual(gdpr['aliases'],['Regulation (EU) 2016/679','General Data Protection Regulation','GDPR'])
        self.assertTrue(gdpr['direct_obligation_asserted'])
        self.assertEqual(gdpr['fields']['jurisdictions'],['EU'])

    def test_scope_and_jurisdictions_do_not_leak_across_instruments(self):
        rows=self.parse('We are not subject to the Alpha Privacy Act in California, but we comply with the UK Bribery Act.')
        laws=[r for r in rows if r['category']=='law']
        self.assertEqual(len(laws),2)
        a,b=laws
        self.assertEqual(a['applicability'],'explicitly_out_of_scope')
        self.assertFalse(a['direct_obligation_asserted'])
        self.assertEqual(a['fields']['jurisdictions'],['California'])
        self.assertEqual(b['fields']['jurisdictions'],['UK'])
        self.assertTrue(b['direct_obligation_asserted'])
        rows=self.parse('Our international risks include compliance with laws such as the Foreign Corrupt Practices Act, UK Bribery Act and other laws.')
        self.assertEqual(next(r for r in rows if r['entity']=='Foreign Corrupt Practices Act')['fields']['jurisdictions'],[])

    def test_third_party_and_proposal_are_not_current_direct_obligations(self):
        rows=self.parse('Our customers are subject to the Alpha Privacy Act, and we supply accounting software.')
        law=next(r for r in rows if r['category']=='law')
        self.assertEqual(law['applicability'],'third_party_context')
        self.assertFalse(law['entity_match_eligible'])
        law=next(r for r in self.parse('We may become subject to the Proposed Streaming Act.') if r['category']=='law')
        self.assertFalse(law['direct_obligation_asserted'])

    def test_generic_litigation_does_not_acquire_an_invented_claimant(self):
        rows=self.parse('The Company is subject to litigation matters and claims, including claims relating to employee relations, business practices and patent infringement.')
        gap=next(r for r in rows if r['relation']=='unnamed_litigation')
        self.assertIn('claimant_not_named',gap['missing_fields'])
        self.assertIn('patent infringement',gap['fields']['disclosed_subjects'])
        self.assertFalse(gap['entity_match_eligible'])
        self.assertFalse(any(r['category']=='named_case' for r in rows))
        self.assertEqual(specificity({'object':'periodic examination'}),'background')

    def test_named_claimant_stays_attached_to_the_claim(self):
        rows=self.parse('A lawsuit was filed against us by Acme Studios alleging copyright infringement.')
        case=next(r for r in rows if r['category']=='named_case')
        self.assertEqual(case['entity'],'Acme Studios')
        self.assertIn('copyright infringement',case['fields']['subject_matter'])

    def test_permit_requires_named_identifier_and_binds_issuing_authority(self):
        rows=self.parse('We must maintain Broadcasting License No. BC-42 issued by the Ministry of Communications in Canada.')
        permit=next(r for r in rows if r['category']=='named_permit')
        self.assertEqual(permit['fields']['permit_identifier'],'BC-42')
        self.assertEqual(permit['fields']['authorities'],['Ministry of Communications'])
        self.assertEqual(permit['fields']['jurisdictions'],['Canada'])
        self.assertTrue(permit['direct_obligation_asserted'])
        rows=self.parse('We require permits to operate our service in Canada.')
        self.assertFalse(any(r['category']=='named_permit' for r in rows))

    def test_a_mentioned_law_is_not_an_asserted_operating_obligation(self):
        law=next(r for r in self.parse('We are discussing the Alpha Privacy Act in our report.') if r['category']=='law')
        self.assertFalse(law['direct_obligation_asserted'])

    def test_full_authority_name_keeps_lowercase_connectors(self):
        rows=self.parse('We are regulated by the Food and Drug Administration.')
        self.assertEqual(next(r for r in rows if r['category']=='authority')['entity'],'Food and Drug Administration')

    def test_examining_authority_and_exact_subject_are_recovered(self):
        rows=self.parse('We are subject to the periodic examination of our domestic and foreign tax returns by the Internal Revenue Service, state, local, and foreign tax authorities.')
        authority=next(r for r in rows if r['category']=='authority')
        self.assertEqual(authority['entity'],'Internal Revenue Service')
        self.assertEqual(authority['relation'],'examined_by')
        self.assertEqual(authority['fields']['subject_matter'],'our domestic and foreign tax returns')
        self.assertIn('named_authorities_only_others_unspecified',authority['missing_fields'])

    def test_primary_nflx_cases_recover_specifics_and_explicit_dma_exclusion(self):
        fixture=json.loads((Path(__file__).resolve().parents[1]/'eval/nflx-specific-disclosure-cases.json').read_text())
        rows=[]
        for case in fixture['cases']:rows+=self.parse(case['text'])
        self.assertTrue(any(r['entity']=='Audio Visual Media Services Directive' and '30%' in r['fields']['reported_quantities'] and r['fields']['jurisdictions']==['EU'] for r in rows),rows)
        dma=next(r for r in rows if r['entity']=='Digital Markets Act')
        self.assertTrue(any(r['entity']=='Regulation (EU) 2016/679' and r['direct_obligation_asserted'] for r in rows))
        self.assertEqual(dma['applicability'],'explicitly_out_of_scope')
        self.assertFalse(dma['direct_obligation_asserted'])
        self.assertTrue(any(r['entity']=='Internal Revenue Service' and r['fields']['subject_matter']=='our domestic and foreign tax returns' for r in rows))

    def test_a_corrupt_detail_span_cannot_be_published(self):
        g=graph('We comply with the Alpha Privacy Act.')
        d=details(g);d['records'][0]['evidence']['anchor']['text']='invented'
        with self.assertRaisesRegex(ValueError,'evidence'):validate_details(g,d)


if __name__=='__main__':unittest.main()
