import hashlib
import pathlib
import sys
import unittest
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))
from src.filing_relationships import load_parser
from src.filing_term_candidates import extract_candidates
from scripts.extract_filing_terms import extract_text


class CandidateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nlp = load_parser()

    def rows(self, text):
        result = extract_candidates(self.nlp(text))
        self.assertEqual(result['input_sha256'], hashlib.sha256(text.encode()).hexdigest())
        for row in result['terms']:
            self.assertEqual(text[row['start']:row['end']], row['text'])
            self.assertEqual(text[row['sentence']['start']:row['sentence']['end']], row['sentence']['text'])
        return result['terms']

    def test_filler_words_are_not_standalone_candidates(self):
        rows = self.rows('We operate factories and warehouses; otherwise, we lease buildings.')
        self.assertFalse({'and','otherwise','we'} & {r['text'].casefold() for r in rows})
        self.assertTrue({'factory','warehouse','building'} <= {r['normalized'] for r in rows})

    def test_plural_nouns_keep_original_text_and_singular_search_key(self):
        rows = self.rows('We manufacture batteries and turbines in our facilities.')
        forms = {r['text']:r['normalized'] for r in rows}
        self.assertEqual(forms['batteries'], 'battery')
        self.assertEqual(forms['turbines'], 'turbine')
        self.assertEqual(forms['facilities'], 'facility')

    def test_named_entities_keep_internal_words_and_plural_names(self):
        rows = self.rows('General Motors and Johnson & Johnson use Bank of America in the United States.')
        forms = {r['text']:r['normalized'] for r in rows}
        for value in ('General Motors','Johnson & Johnson','Bank of America','United States'):
            self.assertEqual(forms[value], value.casefold())

    def test_generic_terms_survive_without_relationship_decisions(self):
        rows = self.rows('Our customers evaluate risks, services and internal systems.')
        self.assertTrue({'customer','risk','service','system'} <= {r['normalized'] for r in rows})
        self.assertTrue(all(r['relationship_status']=='unresolved' for r in rows))

    def test_negation_does_not_remove_a_candidate(self):
        rows = self.rows('We do not manufacture batteries or turbines.')
        self.assertTrue({'battery','turbine'} <= {r['normalized'] for r in rows})

    def test_hyphen_compounds_are_kept_with_their_noun_heads(self):
        rows = self.rows('We provide end-to-end solutions and fiber-to-the-home connections.')
        self.assertTrue({'end-to-end solutions','fiber-to-the-home connections'} <= {r['text'] for r in rows})

    def test_content_word_fallback_keeps_unrecognized_words(self):
        rows = self.rows('Our customer-facing portal (iconnect) improves planning.')
        self.assertIn('iconnect',{r['text'] for r in rows})

    def test_long_inputs_are_complete_and_offsets_remain_valid(self):
        text = ('We manufacture batteries and turbines.\n\n'*450)+'We use warehouses.'
        result = extract_text(text,self.nlp)
        cursor = 0
        for window in result['windows']:
            self.assertEqual(window['start'],cursor)
            cursor = window['end']
        self.assertEqual(cursor,len(text))
        contexts = {c['id']: c for c in result['contexts']}
        for row in result['terms']:
            self.assertEqual(text[row['start']:row['end']], row['text'])
            context = contexts[row['sentence_id']]
            self.assertEqual(text[context['start']:context['end']],context['text'])
        self.assertIn('warehouse',{r['normalized'] for r in result['terms']})
        self.assertEqual(sum(r['text']=='batteries' for r in result['terms']),450)


if __name__ == '__main__':
    unittest.main()
