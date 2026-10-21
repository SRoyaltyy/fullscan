import importlib.util
import unittest
from .stock_keywords import KeywordIndex
from .test_entity_resolution import fixture


@unittest.skipUnless(importlib.util.find_spec('spacy'),'pinned grammar engine is checked in its dedicated CI job')
class PinnedNERControls(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from .filing_relationships import load_parser
        cls.nlp=load_parser()

    def test_grammar_annotations_and_identity_gate_together(self):
        from scripts.annotate_article_entities import annotate
        index=KeywordIndex(fixture())
        positive='Acme Industries CEO John McDonald resigns'
        annotation=annotate(self.nlp(positive),positive)
        self.assertTrue(any(e['label']=='PERSON' and 'McDonald' in e['text'] for e in annotation['mentions']))
        self.assertTrue(any(r['relationship']=='officer' for r in index.compare(positive,entity_annotations=annotation)['matches']))
        for text in ("McDonald's reports restaurant earnings",'McDonalds reports restaurant earnings','Potato chips snack sales rise'):
            annotation=annotate(self.nlp(text),text)
            result=index.compare(text,entity_annotations=annotation)
            self.assertFalse(result['matches'],(text,result))

    def test_actual_ner_complete_names_do_not_resolve_to_nested_aliases(self):
        from scripts.annotate_article_entities import annotate
        index=KeywordIndex(fixture())
        for text in ('Apple Bank raises interest rates on savings accounts.',
                     'Green Apple Restaurants reports strong quarterly earnings.',
                     'Acme Industries signs a music licensing agreement with Apple Records.'):
            annotation=annotate(self.nlp(text),text)
            result=index.compare(text,entity_annotations=annotation)
            self.assertFalse(any(r['relationship']=='supplier' for r in result['matches']),(text,result))
            self.assertTrue(any('different_complete_organization_name' in r['alignment']['reasons']
                                for r in result['rejected_matches']),(text,annotation))

    def test_non_ner_concepts_keep_noun_phrases_and_dependency_attachment(self):
        from scripts.annotate_article_entities import annotate
        text='Wood chips are shipped to a semiconductor plant as boiler fuel.'
        doc=self.nlp(text)
        annotation=annotate(doc,text)
        self.assertTrue(any(p['text']=='Wood chips' for p in annotation['noun_phrases']))
        for token in annotation['syntax']:
            self.assertEqual(text[token['start']:token['end']],token['text'])
            self.assertTrue(0<=token['head']<len(annotation['syntax']))
        wood=next(t for t in annotation['syntax'] if t['text']=='Wood')
        self.assertEqual(annotation['syntax'][wood['head']]['lemma'],'chip')
        with self.assertRaisesRegex(ValueError,'differs'):
            annotate(doc,'Another article')


if __name__=='__main__':unittest.main()
