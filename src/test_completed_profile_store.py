import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from .completed_profile_store import ARCHIVE_COMPILER, validate_manifest, validate_profile
from .specific_filing_records import VERSION, source_fingerprint


def fixture():
    from .test_deep_profile_store import DeepProfileControls
    from scripts.export_completed_parser import project
    graph=DeepProfileControls().graph()
    graph.update(name='Example',parser={'spacy':'preserved'},compiler_sha256=ARCHIVE_COMPILER)
    payload=project(graph)
    raw=gzip.compress(json.dumps(payload).encode(),mtime=0)
    entry={'path':'profiles/0000000001.json.gz','cik':'0000000001',
        'sha256':hashlib.sha256(raw).hexdigest(),'facts':1,'name':'Example'}
    manifest={'schema':'completed-parser-review-1','complete':True,'as_of':graph['as_of'],
        'compiler_sha256':ARCHIVE_COMPILER,'detail_extractor':VERSION,'detail_source_sha256':source_fingerprint(),
        'source_run_id':39522456843,'profiles':{'TEST':entry},
        'asset_base_url':'https://raw.githubusercontent.com/SRoyaltyy/fullscan/'+'a'*40+'/',
        'published':{'tickers':1,'issuers':1,'relationships':1},
        'archive_totals':{'companies':1,'facts':1,'filings':1,'documents':1,'characters':43}}
    return manifest,payload,raw


class CompletedProfileControls(unittest.TestCase):
    def test_current_details_over_old_grammar_are_explicit_and_scoring_compatible(self):
        from .stock_keyword_store import attach_keyword_profiles
        from .stock_evidence import Profile,Catalog,index_profiles
        manifest,graph,raw=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'profiles').mkdir()
            (root/'profiles/0000000001.json.gz').write_bytes(raw)
            (root/'index.json').write_text(json.dumps(manifest))
            board,coverage=attach_keyword_profiles(index_profiles({'TEST':Profile('TEST','Example','','')}),root,
                as_of=manifest['as_of'],catalog=Catalog([]))
            self.assertEqual(coverage['scope'],'archived_relationships_with_current_disclosures')
            self.assertEqual(coverage['covered_tickers'],['TEST'])
            self.assertFalse(coverage['all_prose_dependency_analyzed'])
            self.assertFalse(board.profiles['TEST'].keyword_coverage['term_inventory_available'])
            self.assertEqual(board.profiles['TEST'].keyword_coverage['retained_relationships'],1)
            self.assertEqual(coverage['grammar_compiler_sha256'],ARCHIVE_COMPILER)
            self.assertTrue(all(r['review_required'] for r in board.profiles['TEST'].keyword_records))
            self.assertFalse(any(r['usable_for_scoring'] for r in board.profiles['TEST'].keyword_records))

    def test_program_includes_archive_only_tickers_but_respects_explicit_universe(self):
        from unittest.mock import patch
        from .scanner_backtest import open_program
        manifest,_,raw=fixture()
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory);(root/'profiles').mkdir()
            (root/'profiles/0000000001.json.gz').write_bytes(raw)
            (root/'index.json').write_text(json.dumps(manifest))
            descriptions=root/'stocks.csv';descriptions.write_text('fixture')
            row={'ticker':'LOCAL','name':'Local Industries','description':'Manufactures components','industry':'','sector':''}
            for universe,expected in ((None,{'LOCAL','TEST'}),(root/'universe.csv',{'LOCAL'})):
                with patch('src.scanner_backtest.load_stocks',return_value=[dict(row)]):
                    board,names=open_program(descriptions,None,universe,filing_profiles=root,as_of=manifest['as_of'])
                self.assertEqual(set(board.evidence_board.profiles),expected)
                self.assertEqual(set(board.filing_coverage['covered_tickers']),expected & {'TEST'})
                self.assertEqual(board.filing_coverage['catalog_tickers_without_archived_filings'],1)
                self.assertIsNotNone(names)

    def test_mixed_details_or_missing_issuer_cannot_pass(self):
        manifest,graph,_=fixture()
        validate_manifest(manifest);validate_profile(graph)
        manifest['detail_source_sha256']='stale'
        with self.assertRaisesRegex(ValueError,'current'):validate_manifest(manifest)
        manifest['detail_source_sha256']=source_fingerprint();manifest['profiles']={}
        with self.assertRaisesRegex(ValueError,'omitted'):validate_manifest(manifest)
        graph['specific_records']['paragraph_scope']='review_subset'
        with self.assertRaisesRegex(ValueError,'complete archived'):validate_profile(graph)

    def test_corrupt_original_witness_is_rejected_independently_of_details(self):
        _,graph,_=fixture();graph['facts'][0]['evidence'][0]['quote']='invented'
        with self.assertRaisesRegex(ValueError,'quote differs'):validate_profile(graph)


if __name__=='__main__':unittest.main()
