"""Exact multiword bindings must retain source and question grounding."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.news_impact.meta_hop import normalize_meta

class NounBindings(unittest.TestCase):
    def meta(self, noun, article, question):
        return normalize_meta({'m1': {'need_context': 'no'}, 'm2': [
            {'slot': 'H', 'noun': noun, 'question': question, 'changes': 'direction'}
        ]}, title=article, body='', family='quantity')

    def test_company_name_is_grounded_as_a_whole_phrase(self):
        self.assertIsNotNone(self.meta('General Motors', 'General Motors expands capacity.',
            'What fraction of General Motors capacity is exposed?'))

    def test_phrase_with_stopwords_and_whitespace_is_grounded(self):
        self.assertIsNotNone(self.meta('Bank of America', 'Bank of America faces a new rule.',
            'What fraction of Bank\nof America assets is covered?'))

    def test_unrelated_or_disjoint_names_are_rejected(self):
        self.assertIsNone(self.meta('General Motors', 'General Motors expands capacity.',
            'What fraction of Ford capacity is exposed?'))
        self.assertIsNone(self.meta('General Motors', 'General demand rises. Motors need fuel.',
            'What fraction of General Motors capacity is exposed?'))

    def test_substrings_and_stopword_only_phrases_are_rejected(self):
        self.assertIsNone(self.meta('Meta', 'Metal supply expands.', 'What fraction of Meta costs is exposed?'))
        self.assertIsNone(self.meta('of the', 'Capacity of the factory expands.', 'What fraction of the costs is exposed?'))

if __name__ == '__main__': unittest.main()
