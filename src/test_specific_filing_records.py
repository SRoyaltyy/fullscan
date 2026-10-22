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
        self.assertEqual({r['entity'] for r in laws},{'Regulation (EU) 2016/679','California Privacy Rights Act'})
        self.assertEqual(len({(r['evidence']['anchor']['start'],r['evidence']['anchor']['end']) for r in laws}),len(laws))
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

    def test_plural_instrument_retains_explicitly_attached_administrator(self):
        text='We are subject to U.S. laws and regulations, including the Export Administration Regulations (EAR) administered by the Bureau of Industry and Security (BIS) of the U.S. Department of Commerce.'
        laws=[r for r in self.parse(text) if r['category']=='law']
        self.assertEqual({r['entity'] for r in laws},{'Export Administration Regulations'})
        self.assertEqual(laws[0]['entity'],'Export Administration Regulations')
        self.assertEqual(laws[0]['aliases'],['Export Administration Regulations','EAR'])
        self.assertEqual(laws[0]['fields']['administering_authorities'],['Bureau of Industry and Security'])
        self.assertNotIn('enforcing_authority_not_named_in_this_statement',laws[0]['missing_fields'])

    def test_punctuated_titles_stay_complete_and_lists_stay_separate(self):
        rows=self.parse('We are subject to the Comprehensive Environmental Response, Compensation, and Liability Act of 1980 and the Toxic Substances Control Act of 1976.')
        self.assertEqual([r['entity'] for r in rows if r['category']=='law'],['Comprehensive Environmental Response, Compensation, and Liability Act of 1980','Toxic Substances Control Act of 1976'])
        rows=self.parse('We comply with the Foreign Corrupt Practices Act and UK Bribery Act.')
        self.assertEqual([r['entity'] for r in rows if r['category']=='law'],['Foreign Corrupt Practices Act','UK Bribery Act'])
        self.assertFalse(self.parse('Governmental Regulation.'))

    def test_examining_authority_and_exact_subject_are_recovered(self):
        rows=self.parse('We are subject to the periodic examination of our domestic and foreign tax returns by the Internal Revenue Service, state, local, and foreign tax authorities.')
        authority=next(r for r in rows if r['category']=='authority')
        self.assertEqual(authority['entity'],'Internal Revenue Service')
        self.assertEqual(authority['relation'],'examined_by')
        self.assertEqual(authority['fields']['subject_matter'],'our domestic and foreign tax returns')
        self.assertIn('named_authorities_only_others_unspecified',authority['missing_fields'])

    def test_an_acronym_before_another_law_does_not_become_its_title_or_alias(self):
        rows=self.parse('We may be subject to GDPR, the California Consumer Privacy Act ("CCPA"), and other privacy laws.')
        laws=[r for r in rows if r['category']=='law']
        self.assertEqual({r['entity'] for r in laws},{'California Consumer Privacy Act'})
        self.assertTrue(all('GDPR' not in r['aliases'] for r in laws))
        self.assertEqual(laws[0]['aliases'],['California Consumer Privacy Act','CCPA'])
        self.assertNotIn('GDPR',laws[0]['aliases'])
        self.assertEqual(laws[0]['evidence']['anchor']['text'],'California Consumer Privacy Act')
        self.assertFalse(laws[0]['direct_obligation_asserted'])

    def test_form_fields_contract_headings_and_unnamed_law_classes_are_not_entities(self):
        for title in ('Country Code','Entity Address, Postal Zip Code',
                      'City Area Code','Zip Code','Entity Incorporation State Country Code',
                      'Compliance With Code','Choice of Law','Governing Law',
                      'General Rules and Regulations','Rules and Regulations',
                      'Signature Law','Shareholder Right to Act','Opined-on Law','Bankruptcy Law'):
            with self.subTest(title=title):
                self.assertFalse(any(r['category']=='law' for r in self.parse('Our report includes '+title+'.')))
        rows=self.parse('We comply with the United States Bankruptcy Code and the Internal Revenue Code.')
        self.assertEqual([r['entity'] for r in rows if r['category']=='law'],['United States Bankruptcy Code','Internal Revenue Code'])

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

    def test_unseen_names_follow_the_same_statutory_structure(self):
        for title in ('Zorvan Data Assurance Act','Talven Water Conservation Act',
                      'Internal Revenue Code','Uniform Commercial Code'):
            with self.subTest(title=title):
                self.assertIn(title,[r['entity'] for r in self.parse('We comply with the '+title+'.') if r['category']=='law'])
        for title in ('Device Identification Code','Record Locator Code','Regional Shipping Code'):
            self.assertFalse(any(r['category']=='law' for r in self.parse('Our form contains '+title+'.')))

    def test_numbered_citations_and_unseen_jurisdictions_preserve_identity(self):
        rows=self.parse('We are subject to 47 U.S.C. 310 and the Communications Act of 1934.')
        self.assertIn('47 U.S.C. 310',[r['entity'] for r in rows if r['category']=='law'])
        for country in ('South Africa','Singapore','Norway','South Korea','New Zealand'):
            rows=self.parse('We comply with the Data Assurance Act in '+country+'.')
            self.assertEqual(next(r for r in rows if r['category']=='law')['fields']['jurisdictions'],[country])

    def test_document_definitions_resolve_unseen_aliases_with_separate_evidence(self):
        g=graph('The Zorvan Water Protection Act (the "ZWPA") governs permits.\nWe comply with ZWPA.')
        d=details(g);validate_details(g,d)
        reference=next(r for r in d['records'] if r.get('definition_evidence') and r['direct_obligation_asserted'])
        self.assertEqual(reference['entity'],'Zorvan Water Protection Act')
        self.assertEqual(reference['evidence']['anchor']['text'],'ZWPA')
        self.assertIn('ZWPA',reference['aliases'])
        self.assertEqual(reference['definition_evidence']['name']['text'],'Zorvan Water Protection Act')
        self.assertTrue(reference['direct_obligation_asserted'])

    def test_aliases_never_cross_filings_or_resolve_conflicting_definitions(self):
        text='The Zorvan Privacy Act ("ZPA") and the Zorvan Payments Act ("ZPA") regulate trade. We comply with ZPA.'
        self.assertFalse(any(r.get('definition_evidence') for r in self.parse(text)))
        g=graph('The Zorvan Privacy Act ("ZPA") regulates trade.')
        g['sources'].append({'id':'other','kind':'risk'})
        text='We comply with ZPA.'
        g['paragraphs'].append({'id':'other','source_id':'other','start':0,'end':len(text),'text':text,'kind':'prose'})
        self.assertFalse(any(r.get('definition_evidence') for r in details(g)['records'] if r['source_id']=='other'))

    def test_document_alias_can_cross_sections_but_not_document_or_date(self):
        for changed in (None,'document','filing_date'):
            g=graph('The Zorvan Water Protection Act ("ZWPA") governs permits.')
            g['sources'][0].update(accession='filing-1',document='annual.htm',filing_date='2026-01-01')
            other={**g['sources'][0],'id':'other'}
            if changed:other[changed]='different'
            g['sources'].append(other)
            text='We comply with ZWPA.'
            g['paragraphs'].append({'id':'other','source_id':'other','start':0,'end':len(text),'text':text,'kind':'prose'})
            d=details(g);validate_details(g,d)
            references=[r for r in d['records'] if r['source_id']=='other' and r.get('definition_evidence')]
            self.assertEqual(bool(references),changed is None)

    def test_numbered_rules_and_professional_permits_preserve_exact_holder(self):
        rows=self.parse('We comply with Rules 13a-15(e) and 15d-15(f).')
        self.assertEqual({r['entity'] for r in rows if r['category']=='law'},{'Rules 13a-15(e)','Rule 15d-15(f)'})
        rows=self.parse('The report was prepared by our independent auditor under Professional Permit No. AB-42.')
        permit=next(r for r in rows if r['category']=='named_permit')
        self.assertEqual(permit['applicability'],'third_party_context')
        self.assertFalse(permit['direct_obligation_asserted'])

    def test_auditor_voice_cannot_assert_issuer_compliance(self):
        law=next(r for r in self.parse('We are a public accounting firm and we comply with the Zorvan Audit Act with respect to the Company.') if r['category']=='law')
        self.assertEqual(law['applicability'],'third_party_context')
        self.assertFalse(law['direct_obligation_asserted'])

    def test_parenthesized_country_is_not_an_agency_alias(self):
        rows=self.parse('The Zorvan Audit Authority (United States) (ZAA) issues rules. We are regulated by ZAA. Our customers operate in United States.')
        refs=[r for r in rows if r.get('definition_evidence')]
        self.assertEqual(len(refs),1)
        self.assertEqual(refs[0]['entity'],'Zorvan Audit Authority')
        self.assertEqual(refs[0]['evidence']['anchor']['text'],'ZAA')


if __name__=='__main__':unittest.main()

