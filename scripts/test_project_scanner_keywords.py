import copy
import gzip
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts import project_scanner_keywords as projection


class ProjectionTests(unittest.TestCase):
    def test_workflow_checks_out_every_artifact_required_by_source_validation(self):
        workflow=(projection.ROOT/'.github/workflows/scanner-keyword-publish.yml').read_text()
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
        evidence={r['id']:{'witnesses':[{'quote':'We buy from '+r['keyword']+'.',
            'source':{'filing_date':'2026-01-01'}}]} for r in records}
        payload={'name':'Example','keywords':records,'keyword_evidence':evidence,'keyword_coverage':{},
            'facts':{'supplier':[{'term':'Amazon'}]},'used_for_scoring':True}
        raw=gzip.compress(json.dumps(payload).encode())
        (source/'scoring-profiles/a.json.gz').write_bytes(raw)
        (source/'profiles.json').write_text(json.dumps({'TEST':{'profile_path':'scoring-profiles/a.json.gz',
            'profile_sha256':hashlib.sha256(raw).hexdigest()}}))
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


if __name__=='__main__':unittest.main()
