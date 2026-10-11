"""Government transaction restrictions are not securities venue redesigns."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.news_impact import prompts
from src.news_impact import one_shot_stack as stack
from src.news_impact.meta_hop import meta_acceptable

class AccessMechanism(unittest.TestCase):
    def test_unseen_restrictions_and_relief_get_legal_scope_questions(self):
        for event_class in ('access_control', 'sanction_lift'):
            rows = stack.plan_questions('structure', event_class, 'Government changes transactions with a foreign agency')
            questions = '\n'.join(x['question'] for x in rows)
            self.assertIn('general licenses', questions)
            self.assertIn('wind-down', questions)
            self.assertIn('supplier, customer or ownership', questions)
            self.assertIn('material', questions)
            self.assertNotIn('Who collects the old venue rent', questions)
            self.assertNotIn('Which new venue', questions)

    def test_other_classes_keep_their_original_question_sets(self):
        for family in prompts.FLIP_QUESTIONS:
            self.assertEqual(prompts.event_flip_questions(family, 'market_structure'), prompts.FLIP_QUESTIONS[family])
            self.assertEqual(prompts.event_family_test(family, 'market_structure'), prompts.FAMILY_TESTS[family])
        questions = stack.plan_questions('structure', 'market_structure', 'Securities venue receives an exemptive order')
        self.assertEqual(questions[0]['question'], prompts.FLIP_QUESTIONS['structure'][0])

    def test_all_analyst_paths_use_the_same_access_mechanism(self):
        direct = prompts.analyst_prompt('Transaction restrictions', 'Public reporting.', 'structure', 'access_control', None, 'impulse', 'Restricted transactions')
        staged = stack.analyst_prompt('Transaction restrictions', 'Public reporting.', 'structure', 'access_control', None, 'impulse', 'Restricted transactions', [], [], [], [])
        for text in (direct, staged, prompts.family_block('structure', 'access_control')):
            self.assertIn(prompts.ACCESS_TEST, text)
            self.assertNotIn(prompts.FAMILY_TESTS['structure'], text)
            self.assertIn('Unknown scope', text)
            self.assertIn('need not have a stock ticker', text)

    def test_meta_requests_exceptions_without_inventing_primary_facts(self):
        text = prompts.meta_prompt('Agency restrictions', 'The agency is subject to restrictions.', 'structure', 'access_control', 'Transactions restricted')
        self.assertIn('licenses/exemptions', text)
        self.assertIn('Do not invent the answers', text)
        self.assertNotIn('April', text)
        self.assertNotIn('ICC', text)
        other = prompts.meta_prompt('New trading venue', 'Public reporting.', 'structure', 'market_structure', 'A new venue')
        self.assertNotIn('ACCESS CONTROL CONTEXT', other)

    def test_questions_still_require_article_noun_binding(self):
        meta = {'m1': {'need_context': 'no'}, 'm2': [{'slot': 'C', 'noun': 'agency',
            'question': 'Which agency transactions are authorized by general licenses or exemptions?',
            'changes': 'direction', 'blocks': ['direction']}]}
        self.assertTrue(meta_acceptable(meta, 'Government restricts agency transactions', 'The agency faces new restrictions.', 'structure'))
        self.assertFalse(meta_acceptable(meta, 'Government restricts exports', 'Export restrictions changed.', 'structure'))

    def test_classifier_keeps_proposals_separate_from_operative_controls(self):
        text = prompts.classifier_prompt('Government considers export restrictions', 'An official proposed controls.')
        self.assertIn('proposal or speech alone is statement_public', text)
        self.assertIn('not an operative restriction', text)

if __name__ == '__main__': unittest.main()
