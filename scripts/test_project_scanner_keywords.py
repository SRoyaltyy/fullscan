import copy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import project_scanner_keywords as projection


@patch.object(projection,'ensure_identity_compatibility',lambda *_:None)
class ProjectionTests(unittest.TestCase):
    def test_workflow_checks_out_every_artifact_required_by_source_validation(self):
        workflow=(projection.ROOT/'.github/workflows/scanner-keyword-publish.yml').read_text()
        self.assertIn("'src/stock_keywords.py'",workflow)
        for name in (*projection.ARTIFACTS,'keyword-index.json.gz','keyword-index/',
                     'scoring-profiles/','filing-profiles/index.json','filing-profiles/publication.json'):
            self.assertIn('/dashboard/scanner-backtest/'+name+'\n',workflow)

    def fixture(self, root):
        source=root/'source'; source.mkdir(); (source/'scoring-profiles').mkdir()
        (source/'filing-profiles').mkdir(); (source/'filing-profiles/index.json').write_text('{}')
        def row(name, assertion='asserted'):
            return {'id':name,'stock':'TEST','keyword':name,'normalized':name.lower(),
                'relationship':'supplier','predicate':'buys_from','object_type':'ORG',
                'assertion':assertion,'evidence_ids':[name], 'first_filing_date':'2026-01-01',
                'latest_filing_date':'2026-01-01','qualifiers':{}}
        records=[row('Amazon'),row('business partners'),row('Acme','negated')]
        for r in records:r['identity']={'id':'original:'+r['id'],'aliases':[r['keyword']]}
        evidence={r['id']:{'witnesses':[{'quote':'We buy from '+r['keyword']+'.',
            'source':{'filing_date':'2026-01-01'}}]} for r in records}
        payload={'name':'Example','keywords':records,'keyword_evidence':evidence,'keyword_coverage':{},
            'facts':{'supplier':[{'term':'Amazon'}]},'used_for_scoring':True}
        raw=gzip.compress(json.dumps(payload).encode())
        (source/'scoring-profiles/a.json.gz').write_bytes(raw)
        (source/'profiles.json').write_text(json.dumps({'TEST':{'profile_path':'scoring-profiles/a.json.gz',
            'profile_sha256':hashlib.sha256(raw).hexdigest()}}))
        (source/'keyword-index').mkdir()
        entries=[{'phrase':r['normalized'].split(),'row':copy.deepcopy(r)} for r in records]
        raw=gzip.compress(json.dumps(entries).encode())
        (source/'keyword-index/aa.json.gz').write_bytes(raw)
        index={'schema':projection.SCHEMA,'identity_policy':projection.POLICY,
            'alignment_version':projection.ALIGNMENT_VERSION,'bucket_method':'token_sha256_2',
            'buckets':{'aa':{'path':'keyword-index/aa.json.gz','sha256':hashlib.sha256(raw).hexdigest()}}}
        (source/'keyword-index.json.gz').write_bytes(gzip.compress(json.dumps(index).encode()))
        meta={'snapshot':{'source_revision':'old','snapshot_id':'frozen','keyword_comparison_mode':'browser_preview',
            'filing_coverage':{'covered_tickers':['TEST']}},
            'keyword_index':{'path':'keyword-index.json.gz'}}
        (source/'meta.json').write_text(json.dumps(meta))
        for name in projection.UNCHANGED:(source/name).write_text('old '+name)
        return source,payload,meta

    @patch.object(projection,'validate')
    def test_projection_filters_all_records_and_preserves_scores_and_source_evidence(self, validate):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source,before,meta=self.fixture(root);out=root/'output'
            result=projection.project(source,out,'new')
            self.assertEqual(result['profiles'],1)
            self.assertEqual(result['visible_current_connections'],1)
            after=json.loads(gzip.decompress((out/'scoring-profiles/a.json.gz').read_bytes()))
            self.assertEqual(after['facts'],before['facts'])
            self.assertEqual(after['keyword_evidence'],before['keyword_evidence'])
            self.assertEqual(len(after['keywords']),len(before['keywords']))
            by_name={r['keyword']:r for r in after['keywords']}
            self.assertTrue(by_name['Amazon']['scanner']['eligible'])
            self.assertFalse(by_name['business partners']['scanner']['eligible'])
            self.assertFalse(by_name['Acme']['scanner']['eligible'])
            output=json.loads(gzip.decompress((out/'keyword-index/aa.json.gz').read_bytes()))
            self.assertEqual([e['row']['keyword'] for e in output],['Amazon'])
            self.assertEqual(output[0]['row']['identity'],before['keywords'][0]['identity'])
            self.assertFalse(result['identities_recompiled'])
            updated=json.loads((out/'meta.json').read_text())
            self.assertEqual(updated['snapshot'],meta['snapshot'])
            for name in projection.UNCHANGED:self.assertEqual((source/name).read_bytes(),(out/name).read_bytes())
            self.assertIn('cached engine scores retain their original revision',(out/'index.html').read_text())
            self.assertIn('encodeURIComponent(staticMeta.keyword_projection?.projection_id',(out/'index.html').read_text())
            (out/'results.jsonl').write_text('changed')
            with self.assertRaisesRegex(ValueError,'cached scoring artifact changed'):
                projection.validate_projection(out,'new')

    @patch.object(projection,'validate')
    def test_corrupt_profile_cannot_be_projected(self, validate):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);source,_,_=self.fixture(root)
            (source/'scoring-profiles/a.json.gz').write_bytes(b'corrupt')
            with self.assertRaisesRegex(ValueError,'checksum mismatch'):
                projection.project(source,root/'out','new')


class IdentityCompatibilityTests(unittest.TestCase):
    def test_atomic_main_update_conflict_retries_but_permissions_do_not(self):
        self.assertTrue(projection.retryable_main_race("cannot lock ref 'refs/heads/main': is at "+'a'*40+' but expected '+'b'*40))
        self.assertFalse(projection.retryable_main_race('permission denied: cannot lock ref'))
        self.assertFalse(projection.retryable_main_race('protected branch update rejected'))

    def metadata(self):
        names=('src/entity_resolution.py','src/linguistics.py','src/semantic_lookup.py',
            'src/data/english_words.txt','src/data/lemma_lookup.json.gz')
        return {'snapshot':{'source_revision':'a'*40,'source_hashes':{
            name:projection.digest((projection.ROOT/name).read_bytes()) for name in names}}}

    def test_changed_identity_code_refuses_reuse(self):
        meta=self.metadata();meta['snapshot']['source_hashes']['src/entity_resolution.py']='changed'
        with self.assertRaisesRegex(ValueError,'identity method changed'):
            projection.ensure_identity_compatibility(meta)

    def test_unchanged_compiler_is_reused_but_changed_tokenization_is_rejected(self):
        def original(*args):return (projection.ROOT/args[1].split(':',1)[1]).read_text()
        with patch.object(projection,'git',side_effect=original):
            projection.ensure_identity_compatibility(self.metadata())
        source=(projection.ROOT/'src/stock_keywords.py').read_text().replace("tuple(lemma(m.group())", "tuple(str(m.group())")
        def changed(*args):
            return source if args[1].endswith('stock_keywords.py') else original(*args)
        with patch.object(projection,'git',side_effect=changed):
            with self.assertRaisesRegex(ValueError,'normalization changed'):
                projection.ensure_identity_compatibility(self.metadata())


if __name__=='__main__':unittest.main()
