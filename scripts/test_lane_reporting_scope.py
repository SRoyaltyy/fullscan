"""Publisher UI and absent candidates are not stock-exposure evidence."""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.news_reporting_scope import reporting_scope
from src.news_impact.one_shot_stack import process_article, linker_acceptable
from src.news_impact.finviz_linker import FinvizIndex, candidate_rows

PROMOTION = ('If you rely on our team for trusted reporting, please take a moment to '
    'select us as your Preferred Source on Google , so you will see our news first. '
    'Edited by: Sample Editor Advertisement')

class ReportingScope(unittest.TestCase):
    def test_terminal_promotion_is_excluded_with_exact_offsets(self):
        reporting = 'The agency announced transaction restrictions.'
        raw = reporting + ' ' + PROMOTION
        scoped = reporting_scope(raw)
        self.assertEqual(scoped['body'], reporting)
        self.assertEqual(raw[:scoped['source_body_end']], reporting)
        self.assertGreater(scoped['excluded_characters'], 0)

    def test_genuine_company_reporting_and_nonterminal_text_survive(self):
        reporting = 'Google stopped providing services to the agency.'
        self.assertEqual(reporting_scope(reporting + ' ' + PROMOTION)['body'], reporting)
        raw = 'A publisher described a promotion: ' + PROMOTION + ' Further substantive reporting follows.'
        self.assertEqual(reporting_scope(raw)['body'], raw)

    def test_promotion_cannot_create_a_company_or_its_industry_peer(self):
        rows = [{'ticker': ticker, 'company': name, 'aliases': [name.lower()],
                 'sector': 'Communication Services', 'industry': 'Internet Content & Information',
                 'etf': False, 'market_cap': cap, 'description': '', 'digest': '', 'profile_file': ''}
                for ticker, name, cap in [('GOOG', 'Alphabet Inc', 100), ('META', 'Meta Platforms Inc', 90)]]
        with patch('src.news_impact.finviz_linker.get_index', return_value=FinvizIndex(rows, 'synthetic.csv')):
            def hits(body):
                return {x['ticker'] for x in candidate_rows('Sanctions on a foreign court', body,
                    family='blast', event_class='blast_legal')['instruments']}
            raw = 'The agency announced transaction restrictions. ' + PROMOTION
            self.assertEqual(hits(raw), {'GOOG', 'META'})
            self.assertEqual(hits(reporting_scope(raw)['body']), set())
            self.assertIn('GOOG', hits(reporting_scope('Google stopped court services. ' + PROMOTION)['body']))

    def test_empty_candidate_pool_cannot_request_a_filter_or_assert_no_effect(self):
        calls = []
        def lane(stage, prompt, system, accept=None):
            calls.append(stage)
            if stage == 'classify':
                blob = {'event_class': 'access_control', 'q5': 'impulse', 'sign': None,
                        'constraint': 'Treasury transaction restrictions'}
            elif stage == 'meta':
                blob = {'m1': {'need_context': 'no'}, 'm2': [
                    {'slot': 'H', 'noun': 'agency',
                     'question': 'Which agency counterparties carry the affected payment exposure?',
                     'changes': 'direction'}]}
            else:
                self.fail('No filter/analyst call is justified by an empty candidate pool')
            self.assertTrue(accept(blob))
            return blob, 'synthetic', 'nvidia/nemotron-3-ultra-550b-a55b:free'
        with patch('src.news_impact.one_shot_stack.candidate_rows', return_value={
                'instruments': [], 'finviz_file': 'synthetic.csv'}):
            row = process_article({'title': 'Treasury sanctions a foreign agency',
                'body': 'Treasury restricts transactions with the agency.',
                'published_at': '2026-10-09T00:00:00Z'}, lane, axioms=[], use_pack=False)
        self.assertEqual(calls, ['classify', 'meta'])
        self.assertFalse(row['keep'])
        self.assertEqual(row['reject_reason'], 'no_instrument_candidates')
        self.assertEqual(row['listed_exposure_status'], 'unresolved_by_current_linker')
        self.assertEqual(row['instruments'], [])

    def test_contradictory_or_invented_filter_groups_are_rejected(self):
        self.assertFalse(linker_acceptable({'core': ['GOOG'], 'tangent': ['GOOG']}, [{'ticker': 'GOOG'}]))
        self.assertFalse(linker_acceptable({'core': ['GOOG'], 'tangent': ['MADEUP']}, [{'ticker': 'GOOG'}]))

if __name__ == '__main__': unittest.main()
