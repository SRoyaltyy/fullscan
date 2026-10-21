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


if __name__=='__main__':unittest.main()
