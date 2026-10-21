import copy
import json
import unittest
from types import SimpleNamespace
from .filing_relationships import extract_sections,load_parser,validate_paragraph_coverage
from .stock_keywords import compile_keywords,KeywordIndex
from .term_inventory import parse_terms,validate_article_terms
from scripts.annotate_article_entities import annotate


class TermInventoryControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):cls.nlp=load_parser()

    def graph(self,text,*,sections=None):
        sections=sections or [{'text':text,'kind':'business','filing_date':'2026-01-01',
                              'section':'Item 1','accession':'synthetic','document':'annual.htm'}]
        return extract_sections(sections,ticker='PLBY',name='Playboy Inc.',nlp=self.nlp)

    def test_every_spacy_candidate_is_retained_even_without_relationship_cues(self):
        text='Zebras wander peacefully.\nAn unfamiliar quantum widget glows.\n'
        graph=self.graph(text)
        doc=self.nlp(text)
        expected={(x.start_char,x.end_char) for x in list(doc.ents)+list(doc.noun_chunks)}
        expected|={(t.idx,t.idx+len(t.text)) for t in doc if t.pos_ in ('NOUN','PROPN')}
        rows=graph['term_inventory']['terms']
        self.assertEqual({(r['start'],r['end']) for r in rows},expected)
        self.assertFalse(graph['facts'])
        self.assertTrue(all(r['relationship_status']=='unresolved' for r in rows))
        self.assertTrue(graph['coverage']['all_term_candidates_retained'])
        self.assertEqual(validate_paragraph_coverage(graph),len(graph['paragraphs']))

    def test_identical_word_keeps_distinct_contexts_without_claiming_sense_resolution(self):
        graph=self.graph('We lease office space in Boston.\nOur astronauts train for space missions in orbit.')
        spaces=[r for r in graph['term_inventory']['terms'] if r['text']=='space']
        self.assertEqual(len(spaces),2)
        self.assertEqual({r['context']['noun_phrase']['text'] for r in spaces},{'office space','space missions'})
        self.assertEqual(len({r['context']['fingerprint'] for r in spaces}),2)
        self.assertTrue(all(r['sense_status']=='unresolved' for r in spaces))
        self.assertFalse(graph['term_inventory']['sense_resolution_complete'])

    def test_partner_relationship_links_only_the_actual_argument(self):
        graph=self.graph('We partner with DraftKings while Apex sponsors conferences.')
        rows=graph['term_inventory']['terms']
        partner=next(r for r in rows if r['text']=='DraftKings')
        self.assertEqual(partner['relationships'][0]['predicate'],'partners_with')
        self.assertEqual(partner['relationships'][0]['subject'],'PLBY')
        for row in rows:
            if row['text'] in ('Apex','conferences'):
                self.assertFalse(row['relationships'],row)

    def test_article_capacity_is_separate_from_stock_partner_role(self):
        graph=self.graph('We partner with DraftKings on online gaming promotions.')
        bundle=compile_keywords(graph,as_of='2026-04-01')
        profile=SimpleNamespace(name='Playboy Inc.',keyword_records=bundle['records'],keyword_evidence=bundle['evidence'])
        dkng=SimpleNamespace(name='DraftKings Inc.',keyword_records=[],keyword_evidence={})
        index=KeywordIndex({'PLBY':profile,'DKNG':dkng})
        for text in ('DraftKings faces new gambling regulations.','DraftKings sponsors a charity concert.'):
            result=index.compare(text,entity_annotations=annotate(self.nlp(text),text))
            match=next(r for r in result['matches'] if r['relationship']=='partner')
            self.assertEqual(match['identity']['id'],'org:DKNG')
            self.assertEqual(match['exposure_path']['stock'],'PLBY')
            self.assertEqual(match['exposure_path']['order'],2)
            self.assertFalse(match['exposure_path']['core_industry_inferred'])
            capacity=match['article_capacity']
            self.assertEqual(capacity['dependency'],'nsubj')
            self.assertIn(capacity['governor']['lemma'],('face','sponsor'))
            self.assertTrue(capacity['objects'])
            self.assertFalse(result['stock_passes_changed'])
        result=index.compare('Gambling firms face new regulations.',entity_annotations=annotate(
            self.nlp('Gambling firms face new regulations.'),'Gambling firms face new regulations.'))
        self.assertFalse(any(r['relationship']=='partner' for r in result['matches']))

    def test_dates_filter_term_occurrences_as_well_as_claims(self):
        sections=[{'text':'We partner with DraftKings.','kind':'business','filing_date':date,
                   'accession':date,'document':'annual.htm'} for date in ('2026-01-01','2026-09-01')]
        graph=self.graph('',sections=sections)
        bundle=compile_keywords(graph,as_of='2026-04-01')
        allowed={s['id'] for s in graph['sources'] if s['filing_date']=='2026-01-01'}
        self.assertTrue(bundle['term_inventory']['terms'])
        self.assertTrue(all(r['source_id'] in allowed for r in bundle['term_inventory']['terms']))
        late=compile_keywords(graph,as_of='2026-10-01')
        profile=SimpleNamespace(name='Playboy Inc.',keyword_records=late['records'],keyword_evidence=late['evidence'])
        dkng=SimpleNamespace(name='DraftKings Inc.',keyword_records=[],keyword_evidence={})
        comparison=KeywordIndex({'PLBY':profile,'DKNG':dkng}).compare('DraftKings faces gambling regulations.',article_date='2026-04-01')
        row=next(r for r in comparison['matches'] if r['relationship']=='partner')
        self.assertEqual(set(row['term_occurrence_dates'].values()),{'2026-01-01'})

    def test_cache_relocation_duplicate_sources_and_machine_inventory(self):
        sections=[{'text':'We partner with DraftKings.','kind':'business','filing_date':'2026-01-01'}]*2
        sections+=[{'text':'<xml>123</xml>','kind':'machine_data'}]
        graph=self.graph('',sections=sections)
        self.assertEqual(graph['coverage']['reused_dependency_windows'],1)
        rows=[r for r in graph['term_inventory']['terms'] if r['text']=='DraftKings']
        self.assertEqual(len(rows),2)
        self.assertNotEqual(rows[0]['id'],rows[1]['id'])
        self.assertTrue(all(r['relationships'] for r in rows))
        self.assertFalse(graph['paragraphs'][-1]['term_ids'])
        self.assertEqual(validate_paragraph_coverage(graph),len(graph['paragraphs']))

    def test_missing_term_window_and_forged_relationship_fail_validation(self):
        original=self.graph('We partner with DraftKings.')
        graph=copy.deepcopy(original);graph['term_inventory']['terms'].pop()
        with self.assertRaisesRegex(ValueError,'link mismatch'):validate_paragraph_coverage(graph)
        graph=copy.deepcopy(original);graph['term_inventory']['windows'].clear()
        with self.assertRaisesRegex(ValueError,'whole source'):validate_paragraph_coverage(graph)
        graph=copy.deepcopy(original)
        row=next(r for r in graph['term_inventory']['terms'] if r['relationships'])
        row['relationships'][0]['subject']='OTHER'
        with self.assertRaisesRegex(ValueError,'assertion mismatch'):validate_paragraph_coverage(graph)

    def test_article_capacity_cannot_be_borrowed_from_another_text(self):
        text='DraftKings faces new regulations.'
        payload=annotate(self.nlp(text),text)
        validate_article_terms(text,payload)
        with self.assertRaisesRegex(ValueError,'scored text'):validate_article_terms(text+' Changed',payload)
        payload['terms'][0]['capacity']['governor']['text']='invented'
        with self.assertRaisesRegex(ValueError,'source span'):validate_article_terms(text,payload)

    def test_repeated_runs_are_byte_identical(self):
        text='We partner with DraftKings.\nWe lease office space.'
        self.assertEqual(json.dumps(self.graph(text),sort_keys=True),json.dumps(self.graph(text),sort_keys=True))


if __name__=='__main__':unittest.main()
