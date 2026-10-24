"""Protocol attacks, not semantic-generalization claims or stock-name swaps."""
import copy
import hashlib
import gzip
import json
import tempfile
import unittest
from pathlib import Path
from .scanner_validation import (digest, methodology_fingerprint, reserve,
    validate_register, score_sources, evaluate_modes, release_decision, SCHEMA,
    validate_stored_source_texts)


class ValidationControls(unittest.TestCase):
    def register(self):
        return {'modes': [{'id': 'CLS-01', 'stage': 'classification', 'mode': 'capacity',
                'example': 'original immutable failure', 'expected': 'correct capacity', 'sources': ['audit']}]}

    def reference(self):
        text = 'We purchase copper from Acme.'
        terms = [{'source_id': 's', 'start': 12, 'end': 18, 'text': 'copper',
                  'relationships': [{'subject': 'issuer', 'predicate': 'uses_input', 'object': 'copper',
                    'capacity': 'operating', 'assertion': 'asserted', 'time': 'unknown'}]}]
        return {'reference_basis': 'full_stored_sources', 'reviewer': 'test-only synthetic reference',
                'sources': [{'id': 's', 'text': text, 'date': '2026-10-01',
                    'review_complete': True, 'document_complete': True,
                    'sha256': hashlib.sha256(text.encode()).hexdigest()}], 'terms': terms}

    def test_never_drop_or_rewrite_historical_modes(self):
        old = self.register()
        for field in ('example', 'expected'):
            new = copy.deepcopy(old); new['modes'][0][field] = 'changed'
            with self.assertRaisesRegex(ValueError, 'original failure changed'):
                validate_register(new, old)
        with self.assertRaises(ValueError): validate_register({'modes': []}, old)

    def test_draws_rotate_real_issuers_not_ticker_aliases(self):
        catalog = [{'ticker': str(i), 'cik': str(i//2), 'filing_backed': True} for i in range(20)]
        first, ledger = reserve(catalog, {}, fingerprint='candidate', count=4, seed=1)
        second, ledger = reserve(catalog, ledger, fingerprint='candidate2', count=4, seed=2)
        self.assertFalse({r['issuer_id'] for r in first['issuers']} & {r['issuer_id'] for r in second['issuers']})
        with self.assertRaisesRegex(ValueError, 'exhausted'):
            reserve(catalog, ledger, fingerprint='candidate3', count=4, seed=3)
        self.assertEqual(second['previous_ledger_sha256'], digest({**ledger, 'runs': [first]}))

    def test_known_ticker_excludes_its_other_aliases(self):
        catalog = [{'ticker': t, 'cik': c, 'filing_backed': True}
                   for t, c in [('A','1'), ('ALIAS','1'), ('B','2'), ('C','3')]]
        run, _ = reserve(catalog, {'excluded_tickers': ['A']}, fingerprint='x', count=2, seed=4)
        self.assertNotIn('0000000001', [r['issuer_id'] for r in run['issuers']])

    def test_description_only_rows_cannot_satisfy_filing_coverage(self):
        with self.assertRaisesRegex(ValueError, 'exhausted'):
            reserve([{'ticker':'X','cik':'1','filing_backed':False}], {}, fingerprint='x', count=2)

    def test_methodology_code_change_invalidates_fingerprint(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); (root/'src').mkdir(); (root/'src/a.py').write_text('before')
            before=methodology_fingerprint(root); (root/'src/a.py').write_text('after')
            self.assertNotEqual(before,methodology_fingerprint(root))

    def test_missing_modes_remain_open_and_block_release(self):
        result=release_decision(self.register(),{},fingerprint='now')
        self.assertFalse(result['release_allowed'])
        self.assertEqual(result['modes'][0]['status'],'open')

    def test_extraction_and_classification_do_not_share_score(self):
        ref=self.reference(); actual=copy.deepcopy(ref['terms'])
        actual[0]['relationships'][0]['predicate']='offers'
        result=score_sources(ref,{'terms':actual,'classification_on_verified_terms':actual})
        self.assertEqual(result['extraction_precision'],1)
        self.assertEqual(result['extraction_recall'],1)
        self.assertEqual(result['isolated_classification_accuracy'],0)

    def test_missing_unextracted_term_counts_against_recall(self):
        ref=self.reference(); result=score_sources(ref,{'terms':[]})
        self.assertEqual(result['extraction_recall'],0)
        self.assertFalse(result['extraction_target_met'])
        self.assertEqual(len(result['missed_terms']),1)

    def test_empty_reference_unreviewed_and_partial_source_never_pass(self):
        for field in ('review_complete','document_complete'):
            ref=self.reference(); ref['sources'][0][field]=False
            with self.assertRaisesRegex(ValueError,'incomplete'): score_sources(ref,{'terms':[]})
        ref=self.reference(); ref['terms']=[]
        with self.assertRaisesRegex(ValueError,'empty reference'): score_sources(ref,{'terms':[]})

    def test_modified_evidence_and_partial_term_fail(self):
        ref=self.reference(); ref['sources'][0]['text'] += ' invented'
        with self.assertRaisesRegex(ValueError,'checksum'):score_sources(ref,{'terms':[]})
        ref=self.reference(); actual=copy.deepcopy(ref['terms']); actual[0]['end']-=1
        with self.assertRaisesRegex(ValueError,'original source'):score_sources(ref,{'terms':actual})

    def test_abstention_is_visible_and_not_counted_correct(self):
        ref=self.reference(); isolated=copy.deepcopy(ref['terms']); isolated[0]['relationships']=[]
        result=score_sources(ref,{'terms':ref['terms'],'classification_on_verified_terms':isolated})
        self.assertEqual(result['classification_abstentions'],1)
        self.assertEqual(result['isolated_classification_accuracy'],0)

    def test_wrong_direction_assertion_time_and_capacity_each_fail(self):
        for field in ('subject','predicate','capacity','assertion','time'):
            ref=self.reference(); actual=copy.deepcopy(ref['terms']);actual[0]['relationships'][0][field]='wrong'
            result=score_sources(ref,{'terms':actual,'classification_on_verified_terms':actual})
            self.assertEqual(result['end_to_end_accuracy'],0,field)

    def test_duplicate_occurrences_cannot_inflate_score(self):
        ref=self.reference()
        with self.assertRaisesRegex(ValueError,'duplicate'):
            score_sources(ref,{'terms':ref['terms']*2})

    def test_recursive_check_fails_on_one_bad_descendant(self):
        leaf={'reviewed':True,'evidence_sha256':'x','expected':1,'actual':1}
        check={'checks':[leaf, {**leaf,'actual':0}]}
        row={k:check for k in ('original_reproducer','fresh_real_issuer','useful_positive_control','changed_decisions')}
        row.update(methodology_sha256='x',fresh_run_id='run')
        row['fresh_real_issuer']={**check,'provenance':'stored_source','issuer_id':'B'}
        report={'run_id':'run','reserved_issuers':[{'issuer_id':'B','split':'holdout'}], 'mode_results':{'CLS-01':row}}
        self.assertEqual(evaluate_modes(self.register(),report,fingerprint='x')[0]['status'],'open')

    def test_fake_aggregate_metrics_do_not_override_source_measurement(self):
        ref=self.reference()
        report={'schema':SCHEMA,'methodology_sha256':'x','run_id':'run',
                'candidate_frozen_before_holdout_review':True,'draw_committed_before_review':True,
                'reserved_issuers':[{'issuer_id':'B','split':'holdout'}],
                'source_scores':[{'issuer_id':'B','extraction_precision':1,'extraction_recall':1,
                                  'reference':ref,'predictions':{'terms':[]}}]}
        result=release_decision(self.register(),report,fingerprint='x')
        self.assertTrue(any('below 98%' in r for r in result['reasons']))

    def test_complete_synthetic_protocol_can_pass_but_changed_ledger_cannot(self):
        # Infrastructure fixture only, explicitly not a real-stock claim.
        ref=self.reference()
        source=ref['sources'][0]
        graph={'sources':[{'id':'s','sha256':source['sha256'],'filing_date':source['date'],'characters':len(source['text'])}],
               'paragraphs':[{'source_id':'s','start':0,'end':len(source['text']),'text':source['text']}]}
        raw=gzip.compress(json.dumps(graph).encode(),mtime=0); sha=hashlib.sha256(raw).hexdigest()
        run,ledger=reserve([{'ticker':str(i),'cik':str(i),'filing_backed':True,'source_asset':{'sha256':sha}} for i in range(4)],
                           {},fingerprint='x',count=2,seed=13)
        fresh=next(p['issuer_id'] for p in run['issuers'] if p['split']=='holdout')
        leaf={'reviewed':True,'evidence_sha256':'synthetic-only','expected':1,'actual':1}
        row={k:copy.deepcopy(leaf) for k in ('original_reproducer','fresh_real_issuer','useful_positive_control','changed_decisions')}
        row.update(methodology_sha256='x',fresh_run_id=run['id'])
        row['fresh_real_issuer'].update(issuer_id=fresh,provenance='stored_source')
        row['fresh_real_issuer'].update(source_id='s',start=12,end=18,text='copper',
            evidence_sha256=hashlib.sha256(b'copper').hexdigest())
        row['original_reproducer']['issuer_id']='original'
        report={'schema':SCHEMA,'methodology_sha256':'x','run_id':run['id'],
            'candidate_frozen_before_holdout_review':True,'draw_committed_before_review':True,
            'reserved_issuers':run['issuers'],'mode_results':{'CLS-01':row},
            'source_scores':[{'issuer_id':fresh,'reference':ref,
                'predictions':{'terms':ref['terms'],'classification_on_verified_terms':ref['terms']}}]}
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp)/(sha+'.json.gz')).write_bytes(raw)
            self.assertTrue(release_decision(self.register(),report,fingerprint='x',ledger=ledger,source_root=tmp)['release_allowed'])
            ledger['runs'][0]['seed']=999
            self.assertFalse(release_decision(self.register(),report,fingerprint='x',ledger=ledger,source_root=tmp)['release_allowed'])

    def test_source_integrity_does_not_claim_old_parser_or_semantics_are_complete(self):
        ref=self.reference();s=ref['sources'][0]
        graph={'sources':[{'id':'s','sha256':s['sha256'],'characters':len(s['text'])}],
               'paragraphs':[{'source_id':'s','start':0,'end':len(s['text']),'text':s['text']}],
               'coverage':{'all_prose_dependency_analyzed':False}}
        self.assertEqual(validate_stored_source_texts(graph),{'s':s['text']})
        graph['paragraphs'][0]['start']=1
        with self.assertRaisesRegex(ValueError,'gap'):validate_stored_source_texts(graph)

    def test_article_only_modes_do_not_block_stock_scope_or_count_as_passes(self):
        reg=self.register();reg['modes'][0]['id']='LANE-01'
        result=evaluate_modes(reg,{},fingerprint='x')
        self.assertEqual(result[0]['status'],'out_of_scope')


if __name__=='__main__':unittest.main()
